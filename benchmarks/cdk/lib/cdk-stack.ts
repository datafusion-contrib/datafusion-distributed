import {CfnOutput, RemovalPolicy, Stack, StackProps, Tags} from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3assets from 'aws-cdk-lib/aws-s3-assets';
import * as cr from 'aws-cdk-lib/custom-resources';
import {Construct} from 'constructs';
import * as path from 'path';
import {execSync} from 'child_process';
import {trinoWorkerCommands, trinoUserDataCommands} from "./trino";
import {sparkMasterCommands, sparkWorkerCommands, sparkUserDataCommands} from "./spark";

const ROOT = path.join(__dirname, '../../..')

interface CdkStackProps extends StackProps {
  config: {
    instanceType: string;
    instanceCount: number;
  };
}

export class CdkStack extends Stack {
  constructor(scope: Construct, id: string, props: CdkStackProps) {
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

    // Grant Glue permissions for Trino Hive metastore
    role.addToPolicy(new iam.PolicyStatement({
      actions: [
        'glue:GetDatabase',
        'glue:GetDatabases',
        'glue:GetTable',
        'glue:GetTables',
        'glue:GetPartition',
        'glue:GetPartitions',
        'glue:CreateTable',
        'glue:UpdateTable',
        'glue:DeleteTable',
        'glue:CreateDatabase',
        'glue:UpdateDatabase',
        'glue:DeleteDatabase',
      ],
      resources: ['*'],
    }));

    // Grant read access to the bucket and worker binary
    bucket.grantRead(role);
    workerBinary.grantRead(role);

    // Create EC2 instances
    const instances: ec2.Instance[] = [];
    for (let i = 0; i < config.instanceCount; i++) {
      const userData = ec2.UserData.forLinux();

      userData.addCommands(
        // Install Rust tooling.
        'yum install gcc',
        "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh",
        'cargo install --locked tokio-console',

        // Create systemd service
        `cat > /etc/systemd/system/worker.service << 'EOF'
[Unit]
Description=DataFusion Distributed Worker
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/worker --bucket ${bucket.bucketName}
Restart=always
User=root

[Install]
WantedBy=multi-user.target
EOF`,

        // Enable and start the service
        'systemctl daemon-reload',
        'systemctl enable worker',
        'systemctl start worker',
        ...trinoUserDataCommands(i, this.region),
        ...sparkUserDataCommands(i, this.region)
      );

      const instance = new ec2.Instance(this, `BenchmarkInstance${i}`, {
        vpc,
        vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
        instanceName: `instance-${i}`,
        instanceType: new ec2.InstanceType(config.instanceType),
        machineImage: ec2.MachineImage.latestAmazonLinux2023(),
        securityGroup,
        role,
        userData
      });

      // Tag for peer discovery
      Tags.of(instance).add('BenchmarkCluster', 'datafusion');
      instances.push(instance);
    }

    // Output Session Manager commands for all instances
    new CfnOutput(this, 'ConnectCommands', {
      value: `
# === select one instance to connect to ===
${instances.map(_ => `export INSTANCE_ID=${_.instanceId}`).join("\n")} 

# === port forward the HTTP endpoint ===
aws ssm start-session --target $INSTANCE_ID --document-name AWS-StartPortForwardingSession --parameters "portNumber=9000,localPortNumber=9000"

# === open a sh session in the remote machine ===
aws ssm start-session --target $INSTANCE_ID

# === See worker logs inside a sh session ===
sudo journalctl -u worker.service -f -o cat

`,
      description: 'Session Manager commands to connect to instances',
    });

    // Downloads the latest version of the worker binary and restarts the systemd service.
    // This is done instead of the userData.addS3Download() so that the instance does not need
    // to restart every time a new worker binary is available.
    sendCommandsUnconditionally(this, 'RestartWorkerService', instances, [
      `aws s3 cp s3://${workerBinary.s3BucketName}/${workerBinary.s3ObjectKey} /usr/local/bin/worker`,
      'chmod +x /usr/local/bin/worker',
      'systemctl restart worker',
    ])

    // Then start workers (they will discover the coordinator)
    const [coordinator, ...workers] = instances
    sendCommandsUnconditionally(this, 'TrinoCoordinatorCommands', [coordinator], ['systemctl start trino'])
    sendCommandsUnconditionally(this, 'TrinoWorkerCommands', workers, trinoWorkerCommands(coordinator))

    // Start Spark master and workers
    const [sparkMaster, ...sparkWorkers] = instances
    sendCommandsUnconditionally(this, 'SparkMasterCommands', [sparkMaster], sparkMasterCommands())
    sendCommandsUnconditionally(this, 'SparkWorkerCommands', sparkWorkers, sparkWorkerCommands(sparkMaster))
  }
}

function sendCommandsUnconditionally(
  construct: Construct,
  name: string,
  instances: ec2.Instance[],
  commands: string[]
) {
  const cmd = new cr.AwsCustomResource(construct, name, {
    onUpdate: {
      service: 'SSM',
      action: 'sendCommand',
      parameters: {
        DocumentName: 'AWS-RunShellScript',
        InstanceIds: instances.map(inst => inst.instanceId),
        Parameters: {
          commands
        },
      },
      physicalResourceId: cr.PhysicalResourceId.of(`${name}-${Date.now()}`),
      ignoreErrorCodesMatching: '.*',
    },
    policy: cr.AwsCustomResourcePolicy.fromStatements([
      new iam.PolicyStatement({
        actions: ['ssm:SendCommand'],
        resources: ['*'],
      }),
    ]),
  });

  // Ensure instances are created before restarting
  cmd.node.addDependency(...instances)
}
