import { RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib/core';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { AmazonLinuxCpuType } from 'aws-cdk-lib/aws-ec2';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';

interface CdkStackProps extends StackProps {
  config: {
    instanceType: string;
    instanceCount: number;
  };
}

export class CdkStack extends Stack {
  constructor (scope: Construct, id: string, props: CdkStackProps) {
    super(scope, id, props);

    const { config } = props;

    // Create VPC
    const vpc = new ec2.Vpc(this, 'BenchmarkVPC', {
      maxAzs: 1,
      natGateways: 0,
    });

    // Create security group that allows instances to communicate
    const securityGroup = new ec2.SecurityGroup(this, 'BenchmarkSG', {
      vpc,
      allowAllOutbound: true,
    });

    // Allow all traffic between instances in the same security group
    securityGroup.addIngressRule(
      securityGroup,
      ec2.Port.allTraffic(),
      'Allow all traffic between benchmark instances'
    );

    // Create S3 bucket
    const bucket = new s3.Bucket(this, 'BenchmarkBucket', {
      bucketName: "datafusion-distributed-benchmarks",
      autoDeleteObjects: true,
      removalPolicy: RemovalPolicy.DESTROY
    });

    // Create IAM role for EC2 instances
    const role = new iam.Role(this, 'BenchmarkInstanceRole', {
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
    });

    // Grant read access to the bucket
    bucket.grantRead(role);

    // Create EC2 instances
    for (let i = 0; i < config.instanceCount; i++) {
      new ec2.Instance(this, `BenchmarkInstance${i}`, {
        vpc,
        instanceName: "Distributed DataFusion benchmarking instance",
        instanceType: new ec2.InstanceType(config.instanceType),
        machineImage: ec2.MachineImage.latestAmazonLinux2023({
          cpuType: AmazonLinuxCpuType.ARM_64
        }),
        securityGroup,
        role,
      });
    }
  }
}
