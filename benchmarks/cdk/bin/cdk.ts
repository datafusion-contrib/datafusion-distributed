#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib/core';
import {CdkStack} from '../lib/cdk-stack';
import {DATAFUSION_DISTRIBUTED_ENGINE} from "../lib/datafusion-distributed";
import {TRINO_ENGINE} from "../lib/trino";
import {SPARK_ENGINE} from "../lib/spark";

const app = new cdk.App();

const config = {
    instanceType: 't3.xlarge',
    instanceCount: 4,
    engines: [
        DATAFUSION_DISTRIBUTED_ENGINE,
        SPARK_ENGINE,
        TRINO_ENGINE,
    ]
};

new CdkStack(app, 'DataFusionDistributedBenchmarks', { config });
