# AWS CDK code for DataFusion distributed benchmarks

Creates automatically the appropriate infrastructure in AWS for running benchmarks.

---

# Deploy

## Prerequisites

Cargo zigbuild needs to be installed in the system for cross-compiling to Linux x86_64, which
is what the benchmarking machines in AWS run on.

```shell
cargo install --locked cargo-zigbuild
```

Make sure to also have the `x86_64-unknown-linux-gnu` target installed in
your Rust toolchain:

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

## Authentication setup

Before running AWS/CDK commands, clear stale env credentials and set profile/region:

```shell
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_SECURITY_TOKEN AWS_CREDENTIAL_EXPIRATION
export AWS_PROFILE=<your-profile>
export AWS_REGION=${AWS_REGION:-us-east-1}
export AWS_DEFAULT_REGION="$AWS_REGION"
export AWS_SDK_LOAD_CONFIG=1
```

If using AWS SSO:

```shell
aws sso login --profile "$AWS_PROFILE"
aws sts get-caller-identity --profile "$AWS_PROFILE" --region "$AWS_REGION"
```

## CDK bootstrap and deploy

Bootstrap once per account/region, then deploy:

```shell
ACCOUNT_ID=$(aws sts get-caller-identity --profile "$AWS_PROFILE" --query Account --output text)
npm run bootstrap -- aws://$ACCOUNT_ID/$AWS_REGION
npm run deploy
```

This only needs to be done once in an account/region. After that, `npm run fast-deploy` can be used for fast
deploying new code in seconds.

Deployment writes stack outputs to `.cdk-outputs.json`, which benchmark scripts use for bucket resolution.

## Populating the bucket with TPCH data

Generate TPCH data (from repo root):

```shell
cd ../..
SCALE_FACTOR=10 PARTITIONS=16 ./benchmarks/gen-tpch.sh
cd benchmarks/cdk
```

Then sync:

```shell
npm run sync-bucket
```

---

# Connect to instances

## Prerequisites

The session manager plugin for the AWS CLI needs to be installed, as that's what is used for
connecting to the EC2 machines instead of SSH.

These are the docs with installation instructions:

https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html

On Mac with an Apple Silicon processor, it can be installed with:

```shell
curl "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/mac_arm64/session-manager-plugin.pkg" -o "session-manager-plugin.pkg"
sudo installer -pkg session-manager-plugin.pkg -target
sudo ln -s /usr/local/sessionmanagerplugin/bin/session-manager-plugin /usr/local/bin/session-manager-plugin
```

## Port Forward

Pick one instance ID from stack outputs and port-forward to it:

```shell
INSTANCE_ID=$(aws cloudformation describe-stacks \
  --stack-name DataFusionDistributedBenchmarks \
  --profile "$AWS_PROFILE" \
  --query "Stacks[0].Outputs[?OutputKey=='WorkerInstanceIds'].OutputValue" \
  --output text | cut -d',' -f1)

aws ssm start-session --target "$INSTANCE_ID" \
  --profile "$AWS_PROFILE" \
  --document-name AWS-StartPortForwardingSession \
  --parameters "portNumber=9000,localPortNumber=9000"
```

Just port-forwarding the first instance is enough for issuing queries.

## Connect

Use the same `INSTANCE_ID` from above to connect to the machine:

```shell
aws ssm start-session --target "$INSTANCE_ID" --profile "$AWS_PROFILE"
```

The logs can be streamed with:

```shell
sudo journalctl -u worker.service -f -o cat
```

---

# Running benchmarks

There's a script that will run the TPCH benchmarks against the remote cluster:

In one terminal, perform a port-forward of one machine in the cluster, something like this:

```shell
aws ssm start-session --target "$INSTANCE_ID" \
  --profile "$AWS_PROFILE" \
  --document-name AWS-StartPortForwardingSession \
  --parameters "portNumber=9000,localPortNumber=9000"
```

In another terminal, navigate to the benchmarks/cdk folder:

```shell
cd benchmarks/cdk
```

And run the benchmarking script

```shell
npm run datafusion-bench -- --dataset tpch_sf10
```

Several arguments can be passed for running the benchmarks against different scale factors and with different configs,
for example:

```shell
npm run datafusion-bench -- --dataset tpch_sf10 --iterations 5 --queries q1,q3
```

## Compare branches on remote (full runs)

Run both branches on the same cluster and dataset, then compare:

```shell
cd /path/to/datafusion-distributed
git switch main
cd benchmarks/cdk
npm run fast-deploy
npm run datafusion-bench -- --dataset tpch_sf10

cd /path/to/datafusion-distributed
git switch <feature-branch>
cd benchmarks/cdk
npm run fast-deploy
npm run datafusion-bench -- --dataset tpch_sf10

cd benchmarks/cdk
npm run compare -- --dataset tpch_sf10 datafusion-distributed-main datafusion-distributed-<feature-branch-suffix>
```

Results are stored under `benchmarks/data/<dataset>/.results-remote/<engine>/`.

## Troubleshooting

`Need to perform AWS calls ... but no credentials have been configured`:
- `AWS_PROFILE` / `AWS_REGION` are not set in this shell.

`ExpiredToken` or `InvalidClientTokenId`:
- stale env credentials are overriding SSO; rerun the authentication setup block and login again.

`AWS::EarlyValidation::ResourceExistenceCheck` during deploy:
- likely bucket-name collision; set a unique `BENCHMARK_BUCKET` and redeploy.

`TypeError: fetch failed` / `ECONNREFUSED` on `datafusion-bench`:
- local `9000` port-forward is down or `worker.service` is not healthy.
- verify `curl -sS http://localhost:9000/info` and inspect logs with:
  `sudo journalctl -u worker.service -f -o cat`.
