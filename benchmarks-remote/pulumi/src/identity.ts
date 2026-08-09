import * as aws from '@pulumi/aws';

import { FoundationConfig } from './config';
import { BenchmarkStorage } from './storage';

export interface BenchmarkIdentity {
  workloadRole: aws.iam.Role;
}

const podIdentityAssumeRolePolicy = JSON.stringify({
  Version: '2012-10-17',
  Statement: [
    {
      Effect: 'Allow',
      Principal: { Service: 'pods.eks.amazonaws.com' },
      Action: ['sts:AssumeRole', 'sts:TagSession'],
    },
  ],
});

export function createIdentity(
  config: FoundationConfig,
  storage: BenchmarkStorage,
): BenchmarkIdentity {
  const workloadRole = new aws.iam.Role('benchmark-workload-role', {
    assumeRolePolicy: podIdentityAssumeRolePolicy,
    tags: { Name: `${config.namePrefix}-workloads` },
  });

  new aws.iam.RolePolicy('benchmark-workload-policy', {
    role: workloadRole.id,
    policy: storage.datasetBucket.arn.apply((datasetArn) =>
      JSON.stringify({
        Version: '2012-10-17',
        Statement: [
          {
            Sid: 'ListDatasets',
            Effect: 'Allow',
            Action: ['s3:ListBucket', 's3:GetBucketLocation'],
            Resource: datasetArn,
          },
          {
            Sid: 'ReadDatasets',
            Effect: 'Allow',
            Action: ['s3:GetObject'],
            Resource: `${datasetArn}/*`,
          },
        ],
      }),
    ),
  });

  return { workloadRole };
}
