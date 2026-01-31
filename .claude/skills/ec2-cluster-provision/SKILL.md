---
name: ec2-cluster-provision
description: uses the code present in this repository for provision an EC2 cluster for benchmarking purposes
---

This project uses a remote benchmarks EC2 cluster constructed with AWS CDK located at `benchmarks/cdk`.

There's a package.json file in `benchmarks/cdk/package.json` with relevant commands about deploying.

All the commands in this skill need to be prefixed with whatever the user declared in `./claude/settings.local.json`
in the `aws-commands-prefix` key, typically for providing the commands with the correct permissions.
(e.g., `$aws-commands-prefix npm run deploy` or `$aws-commands-prexfix aws ssm ...`)

Running `npm run cdk deploy` will provision the cluster with the resources specified in `benchmarks/cdk/lib/`.
This takes a while typically (~5 mins). If the user data of the EC2 machines was changed, and you want those changes
to take effect you will need to prepend the deployment command with `USER_DATA_CAUSES_REPLACEMENT=true`.

Once the deployment is complete, the list of instance IDs will be printed to stdout.

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

Remember that for running port forward commands in the background, they take like 5 secs until the
"waiting for connections" message appears. Until then, the port is still not forwarded.
