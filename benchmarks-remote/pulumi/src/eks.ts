import * as aws from '@pulumi/aws';

import { engineNames, FoundationConfig } from './config';
import { BenchmarkIdentity } from './identity';
import { BenchmarkNetwork } from './network';

export interface EksCluster {
  cluster: aws.eks.Cluster;
}

export function createEksCluster(
  config: FoundationConfig,
  network: BenchmarkNetwork,
  identity: BenchmarkIdentity,
): EksCluster {
  const clusterRole = aws.iam.getRoleOutput({ name: 'AmazonEKSAutoClusterRole' });
  const nodeRole = aws.iam.getRoleOutput({ name: 'AmazonEKSAutoNodeRole' });

  const cluster = new aws.eks.Cluster('benchmark-eks-cluster', {
    name: `${config.namePrefix}-eks`,
    roleArn: clusterRole.arn,
    version: config.eksVersion,
    bootstrapSelfManagedAddons: false,
    accessConfig: {
      authenticationMode: 'API',
      bootstrapClusterCreatorAdminPermissions: true,
    },
    computeConfig: {
      enabled: true,
      nodePools: ['system', 'general-purpose'],
      nodeRoleArn: nodeRole.arn,
    },
    kubernetesNetworkConfig: {
      elasticLoadBalancing: { enabled: true },
    },
    storageConfig: {
      blockStorage: { enabled: true },
    },
    zonalShiftConfig: { enabled: true },
    enabledClusterLogTypes: ['api', 'audit', 'authenticator', 'controllerManager', 'scheduler'],
    upgradePolicy: { supportType: 'STANDARD' },
    controlPlaneScalingConfig: { tier: 'standard' },
    vpcConfig: {
      subnetIds: network.privateSubnets.map((subnet) => subnet.id),
      endpointPrivateAccess: true,
      endpointPublicAccess: true,
      publicAccessCidrs: config.kubernetesApiAllowedCidrs,
    },
    tags: { Name: `${config.namePrefix}-eks` },
  });

  for (const engine of engineNames) {
    new aws.eks.PodIdentityAssociation(`benchmark-${engine}-pod-identity`, {
      clusterName: cluster.name,
      namespace: `benchmark-${engine}`,
      serviceAccount: 'benchmark-engine',
      roleArn: identity.workloadRole.arn,
    });
  }

  return { cluster };
}
