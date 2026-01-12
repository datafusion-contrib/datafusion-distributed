import {
    AfterEc2MachinesContext,
    BeforeEc2MachinesContext,
    OnEc2MachinesContext,
    QueryEngine,
    ROOT,
    sendCommandsUnconditionally
} from "./cdk-stack";
import {execSync} from "child_process";
import * as s3assets from "aws-cdk-lib/aws-s3-assets";
import path from "path";

let workerBinary: s3assets.Asset

export const DATAFUSION_DISTRIBUTED_ENGINE: QueryEngine = {
    beforeEc2Machines(ctx: BeforeEc2MachinesContext): void {
        console.log('Building worker binary...');
        execSync('cargo zigbuild -p datafusion-distributed-benchmarks --release --bin worker --target x86_64-unknown-linux-gnu', {
            cwd: ROOT,
            stdio: 'inherit',
        });
        console.log('Worker binary built successfully');


        // Upload worker binary as an asset
        workerBinary = new s3assets.Asset(ctx.scope, 'WorkerBinary', {
            path: path.join(ROOT, 'target/x86_64-unknown-linux-gnu/release/worker'),
        });

        workerBinary.grantRead(ctx.role)
    },
    onEc2Machine(ctx: OnEc2MachinesContext): void {
        ctx.instanceUserData.addCommands(
            'yum install -y gcc openssl-devel',
            'curl --proto \'=https\' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y',

            `aws s3 cp s3://${workerBinary.s3BucketName}/${workerBinary.s3ObjectKey} /usr/local/bin/worker`,
            'chmod +x /usr/local/bin/worker',
            // Create systemd service
            `cat > /etc/systemd/system/worker.service << 'EOF'
[Unit]
Description=DataFusion Distributed Worker
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/worker --bucket ${ctx.bucketName}
Restart=always
User=root

[Install]
WantedBy=multi-user.target
EOF`,

            // Enable and start the service
            'systemctl daemon-reload',
            'systemctl enable worker',
            'systemctl start worker',
        )
    },
    afterEc2Machines(ctx: AfterEc2MachinesContext): void {
        // Downloads the latest version of the worker binary and restarts the systemd service.
        // This is done instead of the userData.addS3Download() so that the instance does not need
        // to restart every time a new worker binary is available.
        sendCommandsUnconditionally(ctx.scope, 'RestartWorkerService',
            ctx.instances,
            [
                `aws s3 cp s3://${workerBinary.s3BucketName}/${workerBinary.s3ObjectKey} /usr/local/bin/worker`,
                'chmod +x /usr/local/bin/worker',
                'systemctl restart worker',
            ])
    }
}