import { RemovalPolicy, Stack, StackProps, Tags, CfnOutput } from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3assets from 'aws-cdk-lib/aws-s3-assets';
import * as cr from 'aws-cdk-lib/custom-resources';
import { Construct } from 'constructs';
import * as path from 'path';
import { execSync } from 'child_process';

const ROOT = path.join(__dirname, '../../..')

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

    // Create VPC with public subnets only (for internet access without NAT gateway)
    const vpc = new ec2.Vpc(this, 'BenchmarkVPC', {
      maxAzs: 1,
      natGateways: 0,
      subnetConfiguration: [
        {
          name: 'Public',
          subnetType: ec2.SubnetType.PUBLIC,
          cidrMask: 24,
        },
      ],
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

    // Build worker binary for Linux
    console.log('Building worker binary...');
    execSync('cargo zigbuild -p datafusion-distributed-benchmarks --release --bin worker --target x86_64-unknown-linux-gnu', {
      cwd: ROOT,
      stdio: 'inherit',
    });
    console.log('Worker binary built successfully');

    // Upload worker binary as an asset
    const workerBinary = new s3assets.Asset(this, 'WorkerBinary', {
      path: path.join(ROOT, 'target/x86_64-unknown-linux-gnu/release/worker'),
    });

    // Create IAM role for EC2 instances
    const role = new iam.Role(this, 'BenchmarkInstanceRole', {
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMManagedInstanceCore'),
      ],
    });

    // Grant permissions to describe EC2 instances (for peer discovery)
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['ec2:DescribeInstances'],
      resources: ['*'],
    }));

    // Grant read access to the bucket, worker binary, and queries
    bucket.grantRead(role);
    workerBinary.grantRead(role);

    // Create EC2 instances
    const instances: ec2.Instance[] = [];
    for (let i = 0; i < config.instanceCount; i++) {
      const userData = ec2.UserData.forLinux();

      userData.addCommands(
        // Extract queries to benchmarking-workspace
        'mkdir -p /home/ec2-user/benchmarking-workspace',
        'unzip -q /tmp/queries.zip -d /home/ec2-user/benchmarking-workspace',
        'chown -R ec2-user:ec2-user /home/ec2-user/benchmarking-workspace',

        // Create startup script that downloads binary
        `cat > /usr/local/bin/start-worker.sh << 'EOF'
#!/bin/bash
set -e

# Download latest worker binary from S3
aws s3 cp s3://${workerBinary.s3BucketName}/${workerBinary.s3ObjectKey} /usr/local/bin/worker
chmod +x /usr/local/bin/worker

# Run worker (it will discover peers itself)
exec /usr/local/bin/worker --bucket ${bucket.bucketName}
EOF`,
        `chmod +x /usr/local/bin/start-worker.sh`,

        // Create systemd service
        `cat > /etc/systemd/system/worker.service << 'EOF'
[Unit]
Description=DataFusion Distributed Worker
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/start-worker.sh
Restart=always
User=root

[Install]
WantedBy=multi-user.target
EOF`,

        // Enable and start the service
        'systemctl daemon-reload',
        'systemctl enable worker',
        'systemctl start worker'
      );

      const instance = new ec2.Instance(this, `BenchmarkInstance${i}`, {
        vpc,
        vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
        instanceName: `instance-${i}`,
        instanceType: new ec2.InstanceType(config.instanceType),
        machineImage: ec2.MachineImage.latestAmazonLinux2023(),
        securityGroup,
        role,
        userData,
      });

      // Tag for peer discovery
      Tags.of(instance).add('BenchmarkCluster', 'datafusion');
      instances.push(instance);
    }

    // Output Session Manager commands for all instances
    new CfnOutput(this, 'ConnectCommands', {
      value: instances.map((inst, i) =>
        `
# instance-${i}
aws ssm start-session --target ${inst.instanceId}
`
      ).join(''),
      description: 'Session Manager commands to connect to instances',
    });

    // Output port forwarding commands
    new CfnOutput(this, 'PortForwardCommands', {
      value: instances.map((inst, i) =>
        `
# instance-${i} (forward port 8000 to localhost:${8000 + i})
aws ssm start-session --target ${inst.instanceId} --document-name AWS-StartPortForwardingSession --parameters "portNumber=8000,localPortNumber=${8000 + i}"
`
      ).join(''),
      description: 'Port forwarding commands (HTTP server on port 8000)',
    });

    // Custom resource to restart worker service on every deploy
    const restartWorker = new cr.AwsCustomResource(this, 'RestartWorkerService', {
      onUpdate: {
        service: 'SSM',
        action: 'sendCommand',
        parameters: {
          DocumentName: 'AWS-RunShellScript',
          InstanceIds: instances.map(inst => inst.instanceId),
          Parameters: {
            commands: ['systemctl restart worker'],
          },
        },
        physicalResourceId: cr.PhysicalResourceId.of(`restart-${Date.now()}`),
      },
      policy: cr.AwsCustomResourcePolicy.fromStatements([
        new iam.PolicyStatement({
          actions: ['ssm:SendCommand'],
          resources: ['*'],
        }),
      ]),
    });

    // Ensure instances are created before restarting
    instances.forEach(inst => restartWorker.node.addDependency(inst));
  }
}
