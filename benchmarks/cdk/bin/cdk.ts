#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib/core';
import {CdkStack} from '../lib/cdk-stack';
import {DATAFUSION_DISTRIBUTED_ENGINE} from "../lib/datafusion-distributed";
import {TRINO_ENGINE} from "../lib/trino";
import {SPARK_ENGINE} from "../lib/spark";
import { BALLISTA_ENGINE } from "../lib/ballista";

const app = new cdk.App();

const config = {
    instanceType: 'c5n.2xlarge',  // Non-burstable, network-optimized
    instanceCount: 4,
    engines: [
        DATAFUSION_DISTRIBUTED_ENGINE,
        SPARK_ENGINE,
        TRINO_ENGINE,
        BALLISTA_ENGINE
    ]
};

new CdkStack(app, 'DataFusionDistributedBenchmarks', {
    config,
    env: { account: process.env.CDK_DEFAULT_ACCOUNT, region: process.env.CDK_DEFAULT_REGION },
});
