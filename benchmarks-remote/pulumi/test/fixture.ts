import { FoundationConfig } from '../src/config';

export function testConfig(): FoundationConfig {
  return {
    namePrefix: 'df-bench',
    region: 'eu-west-1',
    availabilityZones: ['eu-west-1a', 'eu-west-1b'],
    vpcCidr: '10.42.0.0/16',
    subnetCidrs: {
      public: ['10.42.0.0/24', '10.42.1.0/24'],
      private: ['10.42.10.0/24', '10.42.11.0/24'],
    },
    benchmarkInstanceType: 'c5n.2xlarge',
    benchmarkNodeCount: 12,
    benchmarkRootVolumeSizeGiB: 200,
    benchmarkRootVolumeIops: 3000,
    benchmarkRootVolumeThroughput: 125,
    systemInstanceType: 'm6i.large',
    k3sVersion: 'v1.35.1+k3s1',
    k3sPodCidr: '10.244.0.0/16',
  };
}
