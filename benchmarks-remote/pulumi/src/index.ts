import { loadFoundationConfig } from './config';
import { createFoundation } from './foundation';

const foundation = createFoundation(loadFoundationConfig());

export const clusterName = foundation.clusterName;
export const region = foundation.region;
export const datasetBucketName = foundation.datasetBucketName;
export const resultsBucketName = foundation.resultsBucketName;
export const repositoryUrls = foundation.repositoryUrls;
export const imageBuilderProjectName = foundation.imageBuilderProjectName;
export const benchmarkInstanceType = foundation.benchmarkInstanceType;
export const coordinatorInstanceType = foundation.coordinatorInstanceType;
export const benchmarkNodeCount = foundation.benchmarkNodeCount;
