import {CfnOutput, RemovalPolicy, Stack, StackProps, Tags} from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import {Construct} from 'constructs';
import {DATAFUSION_DISTRIBUTED_ENGINE} from "./datafusion-distributed";
import {TRINO_ENGINE} from "./trino";
import {SPARK_ENGINE} from "./spark";
import path from "path";
import * as cr from "aws-cdk-lib/custom-resources";

const USER_DATA_CAUSES_REPLACEMENT = process.env['USER_DATA_CAUSES_REPLACEMENT'] == 'true'
if (USER_DATA_CAUSES_REPLACEMENT) {
    console.warn("Instances will forcefully get replaced")
}

const ENGINES = [
    DATAFUSION_DISTRIBUTED_ENGINE,
    TRINO_ENGINE,
    SPARK_ENGINE
]

export const ROOT = path.join(__dirname, '../../..')

export interface BeforeEc2MachinesContext {
    scope: Construct
    role: iam.Role
}

export interface OnEc2MachinesContext {
    instanceIdx: number
    instanceUserData: ec2.UserData
    region: string
    bucketName: string
}

export interface AfterEc2MachinesContext {
    scope: Construct
    instances: ec2.Instance[]
    bucketName: string
    region: string
}

export interface QueryEngine {
    /** Runs before instantiating any EC2 machine */
    beforeEc2Machines(ctx: BeforeEc2MachinesContext): void

    /** Runs for each instantiated EC2 machine */
    onEc2Machine(ctx: OnEc2MachinesContext): void

    /** Runs after all EC2 machines have been instantiated */
    afterEc2Machines(ctx: AfterEc2MachinesContext): void
}


interface CdkStackProps extends StackProps {
    config: {
        instanceType: string;
        instanceCount: number;
        engines: QueryEngine[]
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

        for (const engine of ENGINES) {
            engine.beforeEc2Machines({
                scope: this,
                role
            })
        }

        // Create EC2 instances
        const instances: ec2.Instance[] = [];
        for (let i = 0; i < config.instanceCount; i++) {
            const userData = ec2.UserData.forLinux();

            for (const engine of ENGINES) {
                engine.onEc2Machine({
                    bucketName: bucket.bucketName,
                    instanceIdx: i,
                    instanceUserData: userData,
                    region: this.region
                })
            }

            const instance = new ec2.Instance(this, `BenchmarkInstance${i}`, {
                vpc,
                vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
                instanceName: `instance-${i}`,
                instanceType: new ec2.InstanceType(config.instanceType),
                machineImage: ec2.MachineImage.latestAmazonLinux2023(),
                securityGroup,
                role,
                userData,
                userDataCausesReplacement: USER_DATA_CAUSES_REPLACEMENT,
                blockDevices: [{
                    deviceName: '/dev/xvda',
                    volume: ec2.BlockDeviceVolume.ebs(200, {
                        volumeType: ec2.EbsDeviceVolumeType.GP3,
                        deleteOnTermination: true,
                    }),
                }],
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

        for (const engine of ENGINES) {
            engine.afterEc2Machines({
                scope: this,
                instances,
                region: this.region,
                bucketName: bucket.bucketName
            })
        }
    }
}

export function sendCommandsUnconditionally(
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
                    commands: [
                        'cloud-init status --wait',
                        ...commands
                    ]
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
