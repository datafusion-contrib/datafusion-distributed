# AWS CDK code for DataFusion distributed benchmarks

Creates automatically the appropriate infrastructure in AWS for running benchmarks.

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

## CDK deploy

```shell
npm run cdk deploy
```

## Populating the bucket with TPCH data

```shell
npm run sync-bucket
```

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

After performing a CDK deploy, a CNF output will be printed to stdout with instructions for port-forwarding
to all the machines, something like this:

```shell
# instance-0 (forward port 8000 to localhost:8000)
aws ssm start-session --target i-04ed9f331dcfae4b6 --document-name AWS-StartPortForwardingSession --parameters "portNumber=8000,localPortNumber=8000"                                     
```

Just port-forwarding the first instance is enough for making queries.

## Connect

After performing a CDK deploy, a CNF output will be printed to stdout with instructions for connecting
to all the machines, something like this:

```shell
# instance-0
aws ssm start-session --target i-00000000000000000
```

Just running one of those commands in the terminal will connect you to the EC2 instance