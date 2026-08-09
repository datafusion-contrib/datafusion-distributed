import * as pulumi from '@pulumi/pulumi';

export const engineNames = ['datafusion', 'ballista', 'spark', 'trino'] as const;

export type EngineName = (typeof engineNames)[number];

export interface SubnetCidrs {
  public: [string, string];
  private: [string, string];
}

export interface FoundationConfig {
  namePrefix: string;
  region: string;
  availabilityZones: [string, string];
  vpcCidr: string;
  subnetCidrs: SubnetCidrs;
  benchmarkInstanceType: string;
  benchmarkNodeCount: number;
  systemInstanceType: string;
  eksVersion: string;
  kubernetesApiAllowedCidrs: string[];
}

function requirePair(name: string, values: string[]): [string, string] {
  if (values.length !== 2) {
    throw new Error(`${name} must contain exactly two entries`);
  }
  if (values[0] === values[1]) {
    throw new Error(`${name} entries must be distinct`);
  }
  return [values[0], values[1]];
}

function requirePositiveInteger(name: string, value: number): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return value;
}

function ipv4CidrRange(name: string, value: string): [number, number] {
  const match = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})\/(\d{1,2})$/.exec(value);
  if (!match) {
    throw new Error(`${name} must be a valid IPv4 CIDR`);
  }
  const octets = match.slice(1, 5).map(Number);
  const prefix = Number(match[5]);
  if (octets.some((octet) => octet > 255) || prefix > 32) {
    throw new Error(`${name} must be a valid IPv4 CIDR`);
  }
  const address = octets.reduce((value, octet) => value * 256 + octet, 0);
  const size = 2 ** (32 - prefix);
  const start = Math.floor(address / size) * size;
  return [start, start + size - 1];
}

export function validateFoundationConfig(config: FoundationConfig): FoundationConfig {
  if (!/^[a-z][a-z0-9-]*$/.test(config.namePrefix) || config.namePrefix.length > 20) {
    throw new Error(
      'namePrefix must start with a lowercase letter, contain only lowercase letters, numbers, and hyphens, and be at most 20 characters',
    );
  }

  requirePair('availabilityZones', config.availabilityZones);
  requirePair('subnetCidrs.public', config.subnetCidrs.public);
  requirePair('subnetCidrs.private', config.subnetCidrs.private);

  requirePositiveInteger('benchmarkNodeCount', config.benchmarkNodeCount);
  if (!/^1\.\d+$/.test(config.eksVersion)) {
    throw new Error('eksVersion must be an exact EKS minor release such as 1.36');
  }
  ipv4CidrRange('vpcCidr', config.vpcCidr);
  if (config.kubernetesApiAllowedCidrs.length === 0) {
    throw new Error('kubernetesApiAllowedCidrs must contain at least one trusted CIDR');
  }
  for (const cidr of config.kubernetesApiAllowedCidrs) {
    ipv4CidrRange('kubernetesApiAllowedCidrs', cidr);
    if (cidr === '0.0.0.0/0') {
      throw new Error('kubernetesApiAllowedCidrs must not expose the Kubernetes API to the world');
    }
  }

  return config;
}

export function loadFoundationConfig(): FoundationConfig {
  const config = new pulumi.Config();
  const awsConfig = new pulumi.Config('aws');
  const apiCidrsFromEnvironment = process.env.KUBERNETES_API_ALLOWED_CIDRS?.split(',')
    .map((cidr) => cidr.trim())
    .filter(Boolean);

  return validateFoundationConfig({
    namePrefix: config.get('namePrefix') ?? 'datafusion-bench',
    region: awsConfig.require('region'),
    availabilityZones: requirePair(
      'availabilityZones',
      config.requireObject<string[]>('availabilityZones'),
    ),
    vpcCidr: config.get('vpcCidr') ?? '10.42.0.0/16',
    subnetCidrs: {
      public: requirePair(
        'subnetCidrs.public',
        config.requireObject<string[]>('publicSubnetCidrs'),
      ),
      private: requirePair(
        'subnetCidrs.private',
        config.requireObject<string[]>('privateSubnetCidrs'),
      ),
    },
    benchmarkInstanceType: config.get('benchmarkInstanceType') ?? 'c5n.2xlarge',
    benchmarkNodeCount: config.getNumber('benchmarkNodeCount') ?? 12,
    systemInstanceType: config.get('systemInstanceType') ?? 'm6i.large',
    eksVersion: config.get('eksVersion') ?? '1.36',
    kubernetesApiAllowedCidrs:
      apiCidrsFromEnvironment && apiCidrsFromEnvironment.length > 0
        ? apiCidrsFromEnvironment
        : config.requireObject<string[]>('kubernetesApiAllowedCidrs'),
  });
}
