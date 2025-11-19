#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib/core';
import { CdkStack } from '../lib/cdk-stack';

const app = new cdk.App();

const config = {
  instanceType: 't3.xlarge',
  instanceCount: 4,
};

new CdkStack(app, 'DataFusionDistributedBenchmarks', { config });
