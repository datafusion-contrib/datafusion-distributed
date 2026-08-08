import * as aws from '@pulumi/aws';

import { FoundationConfig } from './config';

export interface BenchmarkNetwork {
  vpc: aws.ec2.Vpc;
  publicSubnets: [aws.ec2.Subnet, aws.ec2.Subnet];
  privateSubnets: [aws.ec2.Subnet, aws.ec2.Subnet];
}

export function createNetwork(config: FoundationConfig): BenchmarkNetwork {
  const vpc = new aws.ec2.Vpc('benchmark-vpc', {
    cidrBlock: config.vpcCidr,
    enableDnsHostnames: true,
    enableDnsSupport: true,
    tags: {
      Name: `${config.namePrefix}-vpc`,
    },
  });

  const internetGateway = new aws.ec2.InternetGateway('benchmark-internet-gateway', {
    vpcId: vpc.id,
    tags: {
      Name: `${config.namePrefix}-igw`,
    },
  });

  const publicSubnets = config.availabilityZones.map(
    (availabilityZone, index) =>
      new aws.ec2.Subnet(`benchmark-public-${index}`, {
        vpcId: vpc.id,
        availabilityZone,
        cidrBlock: config.subnetCidrs.public[index],
        mapPublicIpOnLaunch: true,
        tags: {
          Name: `${config.namePrefix}-public-${availabilityZone}`,
          'kubernetes.io/role/elb': '1',
        },
      }),
  ) as [aws.ec2.Subnet, aws.ec2.Subnet];

  const privateSubnets = config.availabilityZones.map(
    (availabilityZone, index) =>
      new aws.ec2.Subnet(`benchmark-private-${index}`, {
        vpcId: vpc.id,
        availabilityZone,
        cidrBlock: config.subnetCidrs.private[index],
        tags: {
          Name: `${config.namePrefix}-private-${availabilityZone}`,
          'kubernetes.io/role/internal-elb': '1',
        },
      }),
  ) as [aws.ec2.Subnet, aws.ec2.Subnet];

  const publicRouteTable = new aws.ec2.RouteTable('benchmark-public-routes', {
    vpcId: vpc.id,
    routes: [
      {
        cidrBlock: '0.0.0.0/0',
        gatewayId: internetGateway.id,
      },
    ],
    tags: {
      Name: `${config.namePrefix}-public`,
    },
  });

  publicSubnets.forEach((subnet, index) => {
    new aws.ec2.RouteTableAssociation(`benchmark-public-route-${index}`, {
      routeTableId: publicRouteTable.id,
      subnetId: subnet.id,
    });
  });

  const natAddress = new aws.ec2.Eip(
    'benchmark-nat-address',
    {
      domain: 'vpc',
      tags: {
        Name: `${config.namePrefix}-nat`,
      },
    },
    { dependsOn: [internetGateway] },
  );

  const natGateway = new aws.ec2.NatGateway('benchmark-nat-gateway', {
    allocationId: natAddress.id,
    subnetId: publicSubnets[0].id,
    tags: {
      Name: `${config.namePrefix}-nat`,
    },
  });

  const privateRouteTables = privateSubnets.map(
    (subnet, index) =>
      new aws.ec2.RouteTable(`benchmark-private-routes-${index}`, {
        vpcId: vpc.id,
        routes: [
          {
            cidrBlock: '0.0.0.0/0',
            natGatewayId: natGateway.id,
          },
        ],
        tags: {
          Name: `${config.namePrefix}-private-${config.availabilityZones[index]}`,
        },
      }),
  );

  privateSubnets.forEach((subnet, index) => {
    new aws.ec2.RouteTableAssociation(`benchmark-private-route-${index}`, {
      routeTableId: privateRouteTables[index].id,
      subnetId: subnet.id,
    });
  });

  new aws.ec2.VpcEndpoint('benchmark-s3-endpoint', {
    vpcId: vpc.id,
    serviceName: `com.amazonaws.${config.region}.s3`,
    vpcEndpointType: 'Gateway',
    routeTableIds: privateRouteTables.map((routeTable) => routeTable.id),
    tags: {
      Name: `${config.namePrefix}-s3`,
    },
  });

  return { vpc, publicSubnets, privateSubnets };
}
