import assert from 'node:assert/strict';
import test, { before } from 'node:test';

import * as pulumi from '@pulumi/pulumi';

import { createFoundation } from '../src/foundation';
import { testConfig } from './fixture';

interface RegisteredResource {
  type: string;
  name: string;
  inputs: Record<string, unknown>;
}

const resources: RegisteredResource[] = [];

before(async () => {
  await pulumi.runtime.setMocks(
    {
      newResource(args): { id: string; state: Record<string, unknown> } {
        resources.push({ type: args.type, name: args.name, inputs: args.inputs });

        const state = { ...args.inputs };
        if (args.type === 'aws:s3/bucket:Bucket') {
          state.bucket = `${args.name}-123456`;
          state.arn = `arn:aws:s3:::${state.bucket}`;
        } else if (args.type === 'aws:iam/role:Role') {
          state.name = args.name;
          state.arn = `arn:aws:iam::123456789012:role/${args.name}`;
        } else if (args.type === 'aws:ec2/launchTemplate:LaunchTemplate') {
          state.latestVersion = 1;
        } else if (args.type === 'aws:ecr/repository:Repository') {
          state.repositoryUrl = `123456789012.dkr.ecr.eu-west-1.amazonaws.com/${state.name}`;
          state.arn = `arn:aws:ecr:eu-west-1:123456789012:repository/${state.name}`;
        }

        return { id: `${args.name}_id`, state };
      },
      call(args): Record<string, unknown> {
        if (args.token === 'aws:index/getCallerIdentity:getCallerIdentity') {
          return {
            accountId: '123456789012',
            arn: 'arn:aws:iam::123456789012:user/test',
            userId: 'test',
          };
        }
        if (args.token === 'aws:iam/getRole:getRole') {
          const name = String(args.inputs.name);
          return { ...args.inputs, arn: `arn:aws:iam::123456789012:role/${name}` };
        }
        return args.inputs;
      },
    },
    'datafusion-distributed-benchmarks',
    'test',
    false,
  );

  await pulumi.runtime.runInPulumiStack(async () => {
    return createFoundation(testConfig());
  });
});

test('creates an EKS Auto Mode cluster with both built-in pools', () => {
  const cluster = resources.find((resource) => resource.type === 'aws:eks/cluster:Cluster');
  assert.ok(cluster);
  assert.equal(cluster.inputs.bootstrapSelfManagedAddons, false);
  assert.deepEqual(cluster.inputs.computeConfig, {
    enabled: true,
    nodePools: ['system', 'general-purpose'],
    nodeRoleArn: 'arn:aws:iam::123456789012:role/AmazonEKSAutoNodeRole',
  });
  assert.deepEqual(cluster.inputs.storageConfig, { blockStorage: { enabled: true } });
  assert.deepEqual(cluster.inputs.kubernetesNetworkConfig, {
    elasticLoadBalancing: { enabled: true },
  });
});

test('restricts the public Kubernetes API', () => {
  const cluster = resources.find((resource) => resource.type === 'aws:eks/cluster:Cluster');
  assert.ok(cluster);
  const vpcConfig = cluster.inputs.vpcConfig as Record<string, unknown>;
  assert.deepEqual(vpcConfig.publicAccessCidrs, ['192.0.2.10/32']);
});

test('associates pod identity with every engine namespace', () => {
  const associations = resources.filter(
    (resource) => resource.type === 'aws:eks/podIdentityAssociation:PodIdentityAssociation',
  );
  assert.equal(associations.length, 4);
  assert.ok(
    associations.every((association) => association.inputs.serviceAccount === 'benchmark-engine'),
  );
});

test('keeps result writes out of all cluster roles', async () => {
  const rolePolicies = resources.filter(
    (candidate) => candidate.type === 'aws:iam/rolePolicy:RolePolicy',
  );
  assert.ok(rolePolicies.length > 0);
  for (const resource of rolePolicies) {
    assert.doesNotMatch(String(resource.inputs.policy), /s3:PutObject/);
  }
});

test('scopes managed image publishing to the Spark repository', () => {
  const publisher = resources.find(
    (resource) => resource.name === 'benchmark-image-builder-policy',
  );

  assert.ok(publisher);
  const policy = JSON.parse(String(publisher.inputs.policy)) as {
    Statement: Array<{ Sid: string; Resource: string | string[] }>;
  };
  const publish = policy.Statement.find((statement) => statement.Sid === 'PublishSparkImage');
  assert.ok(publish);
  assert.equal(typeof publish.Resource, 'string');
  assert.doesNotMatch(String(publish.Resource), /\*/);
});
