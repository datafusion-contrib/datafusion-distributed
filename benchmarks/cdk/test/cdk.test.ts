import * as cdk from 'aws-cdk-lib/core';
import * as Cdk from '../lib/cdk-stack';

const config = {
  instanceType: 't3.xlarge',
  instanceCount: 4,
  engines: [],
};

test('it builds', () => {
  const app = new cdk.App();
  // WHEN
  new Cdk.CdkStack(app, 'MyTestStack', {
    config
  });
});
