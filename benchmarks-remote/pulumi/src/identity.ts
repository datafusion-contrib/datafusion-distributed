import * as aws from '@pulumi/aws';
import * as pulumi from '@pulumi/pulumi';

import { FoundationConfig } from './config';
import { BenchmarkStorage } from './storage';

export interface BenchmarkIdentity {
  nodeRole: aws.iam.Role;
  k3sServerRole: aws.iam.Role;
}

const ec2AssumeRolePolicy = JSON.stringify({
  Version: '2012-10-17',
  Statement: [
    {
      Effect: 'Allow',
      Principal: { Service: 'ec2.amazonaws.com' },
      Action: 'sts:AssumeRole',
    },
  ],
});

export function createIdentity(
  config: FoundationConfig,
  storage: BenchmarkStorage,
): BenchmarkIdentity {
  const nodeRole = new aws.iam.Role('benchmark-node-role', {
    assumeRolePolicy: ec2AssumeRolePolicy,
    tags: { Name: `${config.namePrefix}-nodes` },
  });
  new aws.iam.RolePolicyAttachment(
    'benchmark-node-ecr-policy',
    {
      role: nodeRole.name,
      policyArn: aws.iam.ManagedPolicy.AmazonEC2ContainerRegistryReadOnly,
    },
    { aliases: [{ name: 'benchmark-node-policy-0' }] },
  );
  new aws.iam.RolePolicyAttachment(
    'benchmark-node-ssm-policy',
    {
      role: nodeRole.name,
      policyArn: aws.iam.ManagedPolicy.AmazonSSMManagedInstanceCore,
    },
    { aliases: [{ name: 'benchmark-node-policy-1' }] },
  );
  new aws.iam.RolePolicy('benchmark-node-runtime-policy', {
    role: nodeRole.id,
    policy: pulumi.all([storage.datasetBucket.arn]).apply(([datasetArn]) =>
      JSON.stringify({
        Version: '2012-10-17',
        Statement: [
          {
            Sid: 'DiscoverBenchmarkWorkers',
            Effect: 'Allow',
            Action: ['ec2:DescribeInstances'],
            Resource: '*',
          },
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

  const k3sServerRole = new aws.iam.Role('benchmark-k3s-server-role', {
    assumeRolePolicy: ec2AssumeRolePolicy,
    tags: { Name: `${config.namePrefix}-k3s-server` },
  });
  new aws.iam.RolePolicyAttachment(
    'benchmark-k3s-server-ssm-policy',
    {
      role: k3sServerRole.name,
      policyArn: aws.iam.ManagedPolicy.AmazonSSMManagedInstanceCore,
    },
    { aliases: [{ name: 'benchmark-k3s-server-policy-0' }] },
  );
  new aws.iam.RolePolicyAttachment(
    'benchmark-k3s-server-ecr-policy',
    {
      role: k3sServerRole.name,
      policyArn: aws.iam.ManagedPolicy.AmazonEC2ContainerRegistryReadOnly,
    },
    { aliases: [{ name: 'benchmark-k3s-server-policy-1' }] },
  );
  new aws.iam.RolePolicy('benchmark-k3s-bootstrap-policy', {
    role: k3sServerRole.id,
    policy: storage.resultsBucket.arn.apply((resultsArn) =>
      JSON.stringify({
        Version: '2012-10-17',
        Statement: [
          {
            Sid: 'ListBootstrapArtifacts',
            Effect: 'Allow',
            Action: ['s3:ListBucket', 's3:GetBucketLocation'],
            Resource: resultsArn,
            Condition: { StringLike: { 's3:prefix': ['runs/bootstrap/*'] } },
          },
          {
            Sid: 'ReadBootstrapArtifacts',
            Effect: 'Allow',
            Action: ['s3:GetObject'],
            Resource: `${resultsArn}/runs/bootstrap/*`,
          },
        ],
      }),
    ),
  });

  return { nodeRole, k3sServerRole };
}
