import { createFoundation } from './foundation';
import { loadFoundationConfig } from './config';

const foundation = createFoundation(loadFoundationConfig());

export const clusterName = foundation.clusterName;
export const region = foundation.region;
export const datasetBucketName = foundation.datasetBucketName;
export const resultsBucketName = foundation.resultsBucketName;
export const engineNodeGroupNames = foundation.engineNodeGroupNames;
export const repositoryUrls = foundation.repositoryUrls;
export const k3sServerInstanceId = foundation.k3sServerInstanceId;
