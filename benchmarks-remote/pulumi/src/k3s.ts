import * as aws from '@pulumi/aws';
import * as pulumi from '@pulumi/pulumi';
import * as random from '@pulumi/random';

import { engineNames, EngineName, FoundationConfig } from './config';
import { BenchmarkIdentity } from './identity';
import { BenchmarkNetwork } from './network';

export interface K3sCluster {
  clusterName: pulumi.Output<string>;
  serverInstanceId: pulumi.Output<string>;
  engineGroups: Record<EngineName, aws.autoscaling.Group>;
}

function installScript(version: string, command: pulumi.Input<string>): pulumi.Output<string> {
  return pulumi.interpolate`#!/bin/bash
set -euxo pipefail
for attempt in $(seq 1 30); do
  if curl --proto '=https' --tlsv1.2 --retry 5 --retry-all-errors -sfL https://get.k3s.io -o /tmp/install-k3s.sh; then
    break
  fi
  sleep 5
done
test -s /tmp/install-k3s.sh
INSTALL_K3S_VERSION='${version}' ${command}
`;
}

export function createK3sCluster(
  config: FoundationConfig,
  network: BenchmarkNetwork,
  identity: BenchmarkIdentity,
): K3sCluster {
  const clusterName = pulumi.output(`${config.namePrefix}-k3s`);
  const token = new random.RandomPassword('benchmark-k3s-token', {
    length: 48,
    special: false,
  });

  const securityGroup = new aws.ec2.SecurityGroup('benchmark-k3s', {
    vpcId: network.vpc.id,
    description: 'K3s benchmark control plane and workers',
    egress: [
      {
        protocol: '-1',
        fromPort: 0,
        toPort: 0,
        cidrBlocks: ['0.0.0.0/0'],
      },
    ],
    tags: { Name: `${config.namePrefix}-k3s` },
  });
  new aws.ec2.SecurityGroupRule('benchmark-k3s-internal', {
    type: 'ingress',
    protocol: '-1',
    fromPort: 0,
    toPort: 0,
    securityGroupId: securityGroup.id,
    sourceSecurityGroupId: securityGroup.id,
  });

  const serverProfile = new aws.iam.InstanceProfile('benchmark-k3s-server-profile', {
    role: identity.k3sServerRole.name,
  });
  const nodeProfile = new aws.iam.InstanceProfile('benchmark-k3s-node-profile', {
    role: identity.nodeRole.name,
  });
  const ami = aws.ssm.getParameterOutput({
    name: '/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64',
  }).value;
  const serverUserData = installScript(
    config.k3sVersion,
    pulumi.interpolate`K3S_TOKEN='${token.result}' sh /tmp/install-k3s.sh server --cluster-cidr='${config.k3sPodCidr}' --disable=traefik --disable=servicelb --write-kubeconfig-mode=600 --node-label='benchmark.datafusion.apache.org/pool=system'`,
  );
  const server = new aws.ec2.Instance('benchmark-k3s-server', {
    ami,
    instanceType: config.systemInstanceType,
    subnetId: network.publicSubnets[0].id,
    vpcSecurityGroupIds: [securityGroup.id],
    associatePublicIpAddress: true,
    iamInstanceProfile: serverProfile.name,
    userData: serverUserData,
    userDataReplaceOnChange: true,
    metadataOptions: {
      httpEndpoint: 'enabled',
      httpTokens: 'required',
      httpPutResponseHopLimit: 2,
      instanceMetadataTags: 'disabled',
    },
    rootBlockDevice: {
      volumeType: 'gp3',
      volumeSize: 50,
      iops: 3000,
      throughput: 125,
      encrypted: true,
      deleteOnTermination: true,
    },
    tags: {
      Name: `${config.namePrefix}-k3s-server`,
      'benchmark.datafusion.apache.org/pool': 'system',
    },
  });
  const engineGroups = Object.fromEntries(
    engineNames.map((engine) => {
      const labels = {
        'benchmark.datafusion.apache.org/pool': 'engine',
        'benchmark.datafusion.apache.org/engine': engine,
      };
      const agentUserData = installScript(
        config.k3sVersion,
        pulumi.interpolate`K3S_URL='https://${server.privateIp}:6443' K3S_TOKEN='${token.result}' sh /tmp/install-k3s.sh agent --node-label='benchmark.datafusion.apache.org/pool=engine' --node-label='benchmark.datafusion.apache.org/engine=${engine}' --node-taint='benchmark.datafusion.apache.org/engine=${engine}:NoSchedule'`,
      ).apply((script) => Buffer.from(script).toString('base64'));
      const launchTemplate = new aws.ec2.LaunchTemplate(`benchmark-k3s-${engine}`, {
        imageId: ami,
        instanceType: config.benchmarkInstanceType,
        vpcSecurityGroupIds: [securityGroup.id],
        iamInstanceProfile: { arn: nodeProfile.arn },
        userData: agentUserData,
        blockDeviceMappings: [
          {
            deviceName: '/dev/xvda',
            ebs: {
              volumeType: 'gp3',
              volumeSize: config.benchmarkRootVolumeSizeGiB,
              iops: config.benchmarkRootVolumeIops,
              throughput: config.benchmarkRootVolumeThroughput,
              encrypted: 'true',
              deleteOnTermination: 'true',
            },
          },
        ],
        metadataOptions: {
          httpEndpoint: 'enabled',
          httpTokens: 'required',
          httpPutResponseHopLimit: 2,
          instanceMetadataTags: 'disabled',
        },
        tagSpecifications: [
          {
            resourceType: 'instance',
            tags: {
              Name: `${config.namePrefix}-${engine}`,
              ...labels,
              ...(engine === 'datafusion' ? { BenchmarkCluster: 'datafusion' } : {}),
            },
          },
          {
            resourceType: 'volume',
            tags: { Name: `${config.namePrefix}-${engine}`, ...labels },
          },
        ],
        tags: labels,
        updateDefaultVersion: true,
      });
      const group = new aws.autoscaling.Group(
        `benchmark-k3s-${engine}`,
        {
          namePrefix: `${config.namePrefix}-${engine}-`,
          minSize: 0,
          desiredCapacity: 0,
          maxSize: config.benchmarkNodeCount,
          vpcZoneIdentifiers: [network.privateSubnets[0].id],
          launchTemplate: {
            id: launchTemplate.id,
            version: launchTemplate.latestVersion.apply(String),
          },
          tags: Object.entries(labels).map(([key, value]) => ({
            key,
            value,
            propagateAtLaunch: true,
          })),
        },
        { ignoreChanges: ['desiredCapacity'] },
      );
      return [engine, group];
    }),
  ) as Record<EngineName, aws.autoscaling.Group>;

  return {
    clusterName,
    serverInstanceId: server.id,
    engineGroups,
  };
}
