import * as pulumi from '@pulumi/pulumi';

import { FoundationConfig } from './config';
import { createEksCluster } from './eks';
import { createIdentity } from './identity';
import { createImageBuilder } from './image-builder';
import { createNetwork } from './network';
import { createRepositories } from './registry';
import { createStorage } from './storage';

export interface FoundationOutputs {
  clusterName: pulumi.Output<string>;
  region: string;
  datasetBucketName: pulumi.Output<string>;
  resultsBucketName: pulumi.Output<string>;
  repositoryUrls: Record<string, pulumi.Output<string>>;
  imageBuilderProjectName: pulumi.Output<string>;
  benchmarkInstanceType: string;
  coordinatorInstanceType: string;
  benchmarkNodeCount: number;
}

export function createFoundation(config: FoundationConfig): FoundationOutputs {
  const network = createNetwork(config);
  const storage = createStorage(config);
  const identity = createIdentity(config, storage);
  const repositories = createRepositories(config);
  const imageBuilder = createImageBuilder(config, storage, repositories.spark);
  const cluster = createEksCluster(config, network, identity);

  return {
    clusterName: cluster.cluster.name,
    region: config.region,
    datasetBucketName: storage.datasetBucket.bucket,
    resultsBucketName: storage.resultsBucket.bucket,
    repositoryUrls: Object.fromEntries(
      Object.entries(repositories).map(([name, repository]) => [name, repository.repositoryUrl]),
    ),
    imageBuilderProjectName: imageBuilder.name,
    benchmarkInstanceType: config.benchmarkInstanceType,
    coordinatorInstanceType: config.systemInstanceType,
    benchmarkNodeCount: config.benchmarkNodeCount,
  };
}
