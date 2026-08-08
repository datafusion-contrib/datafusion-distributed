import * as aws from '@pulumi/aws';
import * as pulumi from '@pulumi/pulumi';

import { engineNames, FoundationConfig } from './config';
import { createIdentity } from './identity';
import { createK3sCluster } from './k3s';
import { createNetwork } from './network';
import { createRepositories } from './registry';
import { createStorage } from './storage';

export interface FoundationOutputs {
  clusterName: pulumi.Output<string>;
  region: string;
  datasetBucketName: pulumi.Output<string>;
  resultsBucketName: pulumi.Output<string>;
  engineNodeGroupNames: Record<string, pulumi.Output<string>>;
  repositoryUrls: Record<string, pulumi.Output<string>>;
  k3sServerInstanceId: pulumi.Output<string>;
}

export function createFoundation(config: FoundationConfig): FoundationOutputs {
  const network = createNetwork(config);
  const storage = createStorage(config);
  const identity = createIdentity(config, storage);
  const repositories = createRepositories(config);
  new aws.iam.RolePolicy('benchmark-k3s-image-publisher', {
    role: identity.k3sServerRole.id,
    policy: pulumi
      .all(Object.values(repositories).map((repository) => repository.arn))
      .apply((repositoryArns) =>
        JSON.stringify({
          Version: '2012-10-17',
          Statement: [
            {
              Sid: 'AuthenticateToEcr',
              Effect: 'Allow',
              Action: ['ecr:GetAuthorizationToken'],
              Resource: '*',
            },
            {
              Sid: 'PublishBenchmarkImages',
              Effect: 'Allow',
              Action: [
                'ecr:BatchCheckLayerAvailability',
                'ecr:CompleteLayerUpload',
                'ecr:InitiateLayerUpload',
                'ecr:PutImage',
                'ecr:UploadLayerPart',
              ],
              Resource: repositoryArns,
            },
          ],
        }),
      ),
  });
  const cluster = createK3sCluster(config, network, identity);

  return {
    clusterName: cluster.clusterName,
    region: config.region,
    datasetBucketName: storage.datasetBucket.bucket,
    resultsBucketName: storage.resultsBucket.bucket,
    engineNodeGroupNames: Object.fromEntries(
      engineNames.map((engine) => [engine, cluster.engineGroups[engine].name]),
    ),
    repositoryUrls: Object.fromEntries(
      Object.entries(repositories).map(([name, repository]) => [name, repository.repositoryUrl]),
    ),
    k3sServerInstanceId: cluster.serverInstanceId,
  };
}
