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

test('creates one dedicated, zero-sized autoscaling group per engine', async () => {
  const engineNodeGroups = resources.filter(
    (resource) => resource.type === 'aws:autoscaling/group:Group',
  );

  assert.equal(engineNodeGroups.length, 4);
  for (const nodeGroup of engineNodeGroups) {
    const engine = nodeGroup.name.replace('benchmark-k3s-', '');
    assert.equal(nodeGroup.inputs.desiredCapacity, 0);
    assert.equal(nodeGroup.inputs.maxSize, 12);
    assert.equal(nodeGroup.inputs.minSize, 0);
    assert.equal((nodeGroup.inputs.vpcZoneIdentifiers as string[]).length, 1);
    assert.ok(
      (nodeGroup.inputs.tags as Array<{ key: string; value: string }>).some(
        (tag) => tag.key === 'benchmark.datafusion.apache.org/engine' && tag.value === engine,
      ),
    );
  }
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

test('scopes k3s image publishing to benchmark repositories', () => {
  const publisher = resources.find((resource) => resource.name === 'benchmark-k3s-image-publisher');

  assert.ok(publisher);
  const policy = JSON.parse(String(publisher.inputs.policy)) as {
    Statement: Array<{ Sid: string; Resource: string | string[] }>;
  };
  const publish = policy.Statement.find((statement) => statement.Sid === 'PublishBenchmarkImages');
  assert.ok(publish);
  assert.ok(Array.isArray(publish.Resource));
  assert.equal(publish.Resource.length, 4);
  assert.doesNotMatch(JSON.stringify(publish.Resource), /"\*"/);
});
