# AWS CDK code for DataFusion distributed benchmarks

Creates automatically the appropriate infrastructure in AWS for running benchmarks.

---

# Deploy

## Prerequisites

Cargo zigbuild needs to be installed in the system for cross-compiling to Linux x86_64, which is
what the benchmarking machines in AWS run on.

```shell
cargo install --locked cargo-zigbuild
```

Make sure to also have the `x86_64-unknown-linux-gnu` target installed in your Rust toolchain:

```shell
rustup target add x86_64-unknown-linux-gnu
```

Ensure that you can cross-compile to Linux x86_64 before performing any deployments:

```shell
cargo zigbuild -p datafusion-distributed-benchmarks --release --bin worker --target x86_64-unknown-linux-gnu
```

Install JS dependencies:

```shell
cd benchmarks/cdk
npm install
```

## Authentication setup (preferred order)

Use one method per shell session.

Setup references:

- AWS CLI + IAM Identity Center (SSO):
  https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html
- `aws-vault` project: https://github.com/99designs/aws-vault
- AWS CLI profile/config files:
  https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html
- AWS CLI environment variables:
  https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html

### 1) AWS SSO profile commands (preferred)

How to get these values:

- Create or update an SSO profile: `aws configure sso`
- List available profiles: `aws configure list-profiles`
- Inspect a profile's region: `aws configure get region --profile <your-profile>`
- If region is empty, use your target region explicitly (for these benchmarks, `us-east-1` is
  typical)

```shell
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_SECURITY_TOKEN AWS_CREDENTIAL_EXPIRATION
export AWS_PROFILE=<your-profile>
export AWS_REGION=${AWS_REGION:-us-east-1}
export AWS_DEFAULT_REGION="$AWS_REGION"
export AWS_SDK_LOAD_CONFIG=1

aws sso login --profile "$AWS_PROFILE"
aws sts get-caller-identity --profile "$AWS_PROFILE" --region "$AWS_REGION"
```

Use this style with AWS CLI commands:

```shell
aws <service> <operation> --profile "$AWS_PROFILE" --region "$AWS_REGION" ...
```

### 2) Command prefix wrapper (example: `aws-vault`)

How to get these values:

- Use the same `<your-profile>` discovery steps as method 1
- Confirm `aws-vault` can see your profiles: `aws-vault list`
- Verify selected profile identity: `aws-vault exec <your-profile> -- aws sts get-caller-identity`
- Set `AWS_REGION` to your target region (for these benchmarks, `us-east-1` is typical)

```shell
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_SECURITY_TOKEN AWS_CREDENTIAL_EXPIRATION
export AWS_REGION=${AWS_REGION:-us-east-1}
export AWS_DEFAULT_REGION="$AWS_REGION"
awscmd() { aws-vault exec <your-profile> -- "$@"; }

awscmd aws sts get-caller-identity --region "$AWS_REGION"
```

If you use a different command prefix tool, keep the same `awscmd` interface and replace the
function body.

Use this style with AWS CLI commands:

```shell
awscmd aws <service> <operation> --region "$AWS_REGION" ...
```

For npm scripts:

- Use the same wrapper function, for example: `awscmd npm run deploy`

### 3) Explicit environment credentials

Use this when you already have access key credentials.

How to get these values:

- Create or obtain programmatic credentials in AWS IAM (access key ID and secret access key):
  https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html
- For temporary credentials, also obtain a session token from an STS flow (AssumeRole or federation)
- Set `AWS_REGION` to the target region before running commands

```shell
unset AWS_PROFILE
export AWS_ACCESS_KEY_ID=<access-key-id>
export AWS_SECRET_ACCESS_KEY=<secret-access-key>
# Include session token when using temporary credentials:
# export AWS_SESSION_TOKEN=<session-token>
export AWS_REGION=${AWS_REGION:-us-east-1}
export AWS_DEFAULT_REGION="$AWS_REGION"
```

Validate credentials:

```shell
aws sts get-caller-identity --region "$AWS_REGION"
```

## CDK bootstrap and deploy

Bootstrap once per account/region, then deploy:

```shell
# Method 1: AWS SSO profile commands
ACCOUNT_ID=$(aws sts get-caller-identity --profile "$AWS_PROFILE" --region "$AWS_REGION" --query Account --output text)
npm run bootstrap -- aws://$ACCOUNT_ID/$AWS_REGION
npm run deploy

# Method 2: Command prefix wrapper (example: aws-vault)
ACCOUNT_ID=$(awscmd aws sts get-caller-identity --region "$AWS_REGION" --query Account --output text)
awscmd npm run bootstrap -- aws://$ACCOUNT_ID/$AWS_REGION
awscmd npm run deploy

# Method 3: Explicit environment credentials
ACCOUNT_ID=$(aws sts get-caller-identity --region "$AWS_REGION" --query Account --output text)
npm run bootstrap -- aws://$ACCOUNT_ID/$AWS_REGION
npm run deploy
```

This only needs to be done once in an account/region. After that, `npm run fast-deploy` can be used
for fast deploying new code in seconds.

Deployment writes stack outputs to `.cdk-outputs.json`, which benchmark scripts use for bucket
resolution.

## Populating the bucket with TPCH data

Generate TPCH data (from repo root):

```shell
cd ../..
SCALE_FACTOR=10 PARTITIONS=16 ./benchmarks/gen-tpch.sh
cd benchmarks/cdk
```

Then sync:

```shell
# Method 1 and 3:
npm run sync-bucket

# Method 2:
awscmd npm run sync-bucket
```

---

# Connect to instances

## Prerequisites

The session manager plugin for the AWS CLI needs to be installed, as that's what is used for
connecting to the EC2 machines instead of SSH.

These are the docs with installation instructions:

https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html

On Mac, it can be installed with:

```shell
curl "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/mac_arm64/session-manager-plugin.pkg" -o "session-manager-plugin.pkg"
sudo installer -pkg session-manager-plugin.pkg -target /
sudo ln -sf /usr/local/sessionmanagerplugin/bin/session-manager-plugin /usr/local/bin/session-manager-plugin
session-manager-plugin --version
```

# Running benchmarks

Use this flow in order: port-forward, health check, then benchmark run.

1. In terminal A, pick one instance and start a local port-forward:

```shell
# Method 1: AWS SSO profile commands
INSTANCE_ID=$(aws cloudformation describe-stacks \
  --stack-name DataFusionDistributedBenchmarks \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='WorkerInstanceIds'].OutputValue" \
  --output text | cut -d',' -f1)

aws ssm start-session --target "$INSTANCE_ID" \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --document-name AWS-StartPortForwardingSession \
  --parameters "portNumber=9000,localPortNumber=9000"

# Method 2: Command prefix wrapper (example: aws-vault)
INSTANCE_ID=$(awscmd aws cloudformation describe-stacks \
  --stack-name DataFusionDistributedBenchmarks \
  --region "$AWS_REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='WorkerInstanceIds'].OutputValue" \
  --output text | cut -d',' -f1)

awscmd aws ssm start-session --target "$INSTANCE_ID" \
  --region "$AWS_REGION" \
  --document-name AWS-StartPortForwardingSession \
  --parameters "portNumber=9000,localPortNumber=9000"

# Method 3: Explicit environment credentials
INSTANCE_ID=$(aws cloudformation describe-stacks \
  --stack-name DataFusionDistributedBenchmarks \
  --region "$AWS_REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='WorkerInstanceIds'].OutputValue" \
  --output text | cut -d',' -f1)

aws ssm start-session --target "$INSTANCE_ID" \
  --region "$AWS_REGION" \
  --document-name AWS-StartPortForwardingSession \
  --parameters "portNumber=9000,localPortNumber=9000"
```

Keep this command running. It should print `Waiting for connections...`.

2. In terminal B, verify the service is healthy:

```shell
curl -sS http://localhost:9000/info | jq .
```

If terminal B is a new shell session, re-run your auth setup first (and re-define `awscmd` for
method 2), otherwise environment variables from terminal A will not carry over.

3. In terminal B, run benchmarks from `benchmarks/cdk`:

```shell
cd benchmarks/cdk

# Method 1 and 3
npm run datafusion-bench -- --dataset tpch_sf10

# Method 2
awscmd npm run datafusion-bench -- --dataset tpch_sf10
```

## Optional: connect directly to an instance for debugging

Use this only for manual inspection/debugging (not required for normal benchmark runs).

```shell
# Method 1: AWS SSO profile commands
aws ssm start-session --target "$INSTANCE_ID" --profile "$AWS_PROFILE" --region "$AWS_REGION"

# Method 2: Command prefix wrapper (example: aws-vault)
awscmd aws ssm start-session --target "$INSTANCE_ID" --region "$AWS_REGION"

# Method 3: Explicit environment credentials
aws ssm start-session --target "$INSTANCE_ID" --region "$AWS_REGION"
```

Inside the instance shell, stream worker logs with:

```shell
sudo journalctl -u worker.service -f -o cat
```

Several arguments can be passed for running the benchmarks against different scale factors and with
different configs, for example:

```shell
npm run datafusion-bench -- --dataset tpch_sf10 --iterations 5 --queries q1,q3
```

## Compare branches on remote (full runs)

Run both branches on the same cluster and dataset, then compare:

```shell
cd /path/to/datafusion-distributed
git switch main
cd benchmarks/cdk
# Method 1 and 3
npm run fast-deploy
npm run datafusion-bench -- --dataset tpch_sf10
# Method 2
# awscmd npm run fast-deploy
# awscmd npm run datafusion-bench -- --dataset tpch_sf10

cd /path/to/datafusion-distributed
git switch <feature-branch>
cd benchmarks/cdk
# Method 1 and 3
npm run fast-deploy
npm run datafusion-bench -- --dataset tpch_sf10
# Method 2
# awscmd npm run fast-deploy
# awscmd npm run datafusion-bench -- --dataset tpch_sf10

cd benchmarks/cdk
# Method 1 and 3
npm run compare -- --dataset tpch_sf10 datafusion-distributed-main datafusion-distributed-<feature-branch-suffix>
# Method 2
# awscmd npm run compare -- --dataset tpch_sf10 datafusion-distributed-main datafusion-distributed-<feature-branch-suffix>
```

Results are stored under `benchmarks/data/<dataset>/.results-remote/<engine>/`.

## Troubleshooting

`Need to perform AWS calls ... but no credentials have been configured`:

- Auth setup for your chosen method is incomplete.
- For method 1, verify `AWS_PROFILE` and `AWS_REGION`.
- For method 2, verify the `awscmd` shell function and `AWS_REGION`.
- For method 3, verify `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_REGION`.

`ExpiredToken` or `InvalidClientTokenId`:

- stale credentials are active; rerun your chosen authentication setup and validate with
  `aws sts get-caller-identity`.

`AWS::EarlyValidation::ResourceExistenceCheck` during deploy:

- likely bucket-name collision; set a unique `BENCHMARK_BUCKET` and redeploy.

`TypeError: fetch failed` / `ECONNREFUSED` on `datafusion-bench`:

- local `9000` port-forward is down or `worker.service` is not healthy.
- verify `curl -sS http://localhost:9000/info` and inspect logs with:
  `sudo journalctl -u worker.service -f -o cat`.
