---
name: remote-benchmark
description: deploys the code to a remote EC2 cluster with the commands available in the package.json, port-forwards
 a machine port, and runs benchmarks against it.
---

This project uses a remote benchmarks EC2 cluster constructed with AWS CDK located at `benchmarks/cdk`.

There's a package.json file in `benchmarks/cdk/package.json` with relevant commands about benchmarking.

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

Before benchmarking, ensure shell auth is valid for the chosen method:

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

If this is the first run in an account/region, bootstrap and deploy once:

```shell
# Method 1: AWS SSO profile commands
ACCOUNT_ID=$(aws sts get-caller-identity --profile "$AWS_PROFILE" --query Account --output text)
npm run bootstrap -- aws://$ACCOUNT_ID/$AWS_REGION
npm run deploy
npm run sync-bucket

# Method 2: Command prefix wrapper (example: aws-vault)
ACCOUNT_ID=$(awscmd aws sts get-caller-identity --region "$AWS_REGION" --query Account --output text)
awscmd npm run bootstrap -- aws://$ACCOUNT_ID/$AWS_REGION
awscmd npm run deploy
awscmd npm run sync-bucket

# Method 3: Explicit environment credentials
ACCOUNT_ID=$(aws sts get-caller-identity --region "$AWS_REGION" --query Account --output text)
npm run bootstrap -- aws://$ACCOUNT_ID/$AWS_REGION
npm run deploy
npm run sync-bucket
```

You can assume that the cluster is already there, and the only thing necessary is to execute the `npm run fast-deploy`
command for deploying the current code to the EC2 cluster. Remember that all npm commands need to be run from the
`benchmarks/cdk` folder.

The `npm run fast-deploy` command will compile the current code and deploy it to the EC2 machines. If it fails,
prompt the user to fix it. It will output several EC2 instance IDs: pick the first one, that's the one we will port
forward locally in order to issue queries to it.

Once deployment is completed, get one instance ID and port forward it to local port 9000:

```shell
# Method 1: AWS SSO profile commands
INSTANCE_ID=$(aws cloudformation describe-stacks \
  --stack-name DataFusionDistributedBenchmarks \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='WorkerInstanceIds'].OutputValue" \
  --output text | cut -d',' -f1)
aws ssm start-session --target "$INSTANCE_ID" --profile "$AWS_PROFILE" --region "$AWS_REGION" --document-name AWS-StartPortForwardingSession --parameters "portNumber=9000,localPortNumber=9000"

# Method 2: Command prefix wrapper (example: aws-vault)
INSTANCE_ID=$(awscmd aws cloudformation describe-stacks \
  --stack-name DataFusionDistributedBenchmarks \
  --region "$AWS_REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='WorkerInstanceIds'].OutputValue" \
  --output text | cut -d',' -f1)
awscmd aws ssm start-session --target "$INSTANCE_ID" --region "$AWS_REGION" --document-name AWS-StartPortForwardingSession --parameters "portNumber=9000,localPortNumber=9000"

# Method 3: Explicit environment credentials
INSTANCE_ID=$(aws cloudformation describe-stacks \
  --stack-name DataFusionDistributedBenchmarks \
  --region "$AWS_REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='WorkerInstanceIds'].OutputValue" \
  --output text | cut -d',' -f1)
aws ssm start-session --target "$INSTANCE_ID" --region "$AWS_REGION" --document-name AWS-StartPortForwardingSession --parameters "portNumber=9000,localPortNumber=9000"
```

Remember to run that in the background, as that will block in place.

Once the port is correctly listening locally (you will see a "waiting for connections" message), it's fine to start
the benchmarks.

You can start full TPCH benchmarks with:

```shell
npm run datafusion-bench -- --dataset tpch_sf10
```

You can learn how this command works by running:

```shell
$ npm run datafusion-bench -- --help

Usage: datafusion-bench [options]

Options:
  --dataset <string>                   Dataset to run queries on
  -i, --iterations <number>            Number of iterations (default: "3")
  --files-per-task <number>            Files per task (default: "8")
  --cardinality-task-sf <number>       Cardinality task scale factor (default: "1")
  --batch-size <number>                Standard Batch coalescing size (number of rows) (default: "32768")
  --shuffle-batch-size <number>        Shuffle batch coalescing size (number of rows) (default: "32768")
  --children-isolator-unions <number>  Use children isolator unions (default: "true")
  --broadcast-joins <boolean>          Use broadcast joins (default: "false")
  --collect-metrics <boolean>          Propagates metric collection (default: "true")
  --compression <string>               Compression algo to use within workers (lz4, zstd, none) (default: "lz4")
  --queries <string>                   Specific queries to run
  --debug <boolean>                    Print the generated plans to stdout
  --warmup <boolean>                   Perform a warmup query before the benchmarks (default: "true")
  -h, --help                           display help for command
```

The --dataset command is mandatory, and its value can be any of the folder names in `benchmarks/data`, for example:
clickbench_0-100, tpcds_sf1, tpch_sf1, tpch_sf10 or tpch_sf100.

Also, the --queries argument can be used for executing just a partial subset of queries, for example:
```shell
--queries q1,q2,q3
```

When benchmarking a very specific feature, it's convenient to choose wisely a relevant query and just execute that one.

The user provided the following arguments: $ARGUMENTS

parse those and make sure you parse them correctly, for example `tpch_sf100 q1,q2,q4` means 
`--dataset tpch_sf100 --queries q1,q2,q4`. Note that the user might also give natural language instructions in the 
arguments, be smart while parsing those.

### analyzing results

results for individual queries will be dumped in the respective dataset folders, for example:

`benchmarks/data/tpch_sf10/.results-remote/datafusion-distributed-main/q1.json`
or
`benchmarks/data/tpch_sf1/.results-remote/datafusion-distributed-new-branch/q2.json`

You can inspect the results and the plan by reading the JSONs. Tip: use jq for printing nice results.

As the results of previous branches are already stored in disk, they usually can be analyzed without re-running them
again, that can be done by either:
- Just looking at the latencies and plans in the output folders.
- Running `npm run compare -- --dataset tpch_sfX datafusion-distributed-<base branch> datafusion-distributed-<compare branch>`
