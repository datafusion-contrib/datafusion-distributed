import {
    AfterEc2MachinesContext,
    BeforeEc2MachinesContext,
    QueryEngine,
    ROOT,
    sendCommandsUnconditionally,
    OnEc2MachinesContext
} from "./cdk-stack";
import * as s3assets from "aws-cdk-lib/aws-s3-assets";
import path from "path";
import {execSync} from "child_process";

let ballistaServerBinary: s3assets.Asset
let ballistaSchedulerBinary: s3assets.Asset
let ballistaExecutorBinary: s3assets.Asset

// Ballista is built as a standalone project, separate from the main workspace
const BALLISTA_DIR = path.join(ROOT, 'benchmarks/cdk/ballista')

export const BALLISTA_ENGINE: QueryEngine = {
    beforeEc2Machines(ctx: BeforeEc2MachinesContext): void {
        console.log('Building Ballista binaries (standalone project)...');
        execSync('cargo zigbuild --release --target x86_64-unknown-linux-gnu', {
            cwd: BALLISTA_DIR,
            stdio: 'inherit',
        });
        console.log('Ballista binaries built successfully');

        const targetDir = path.join(BALLISTA_DIR, 'target/x86_64-unknown-linux-gnu/release')

        ballistaServerBinary = new s3assets.Asset(ctx.scope, 'BallistaServerBinary', {
            path: path.join(targetDir, 'ballista-http'),
        })

        ballistaSchedulerBinary = new s3assets.Asset(ctx.scope, 'BallistaSchedulerBinary', {
            path: path.join(targetDir, 'ballista-scheduler'),
        })

        ballistaExecutorBinary = new s3assets.Asset(ctx.scope, 'BallistaExecutorBinary', {
            path: path.join(targetDir, 'ballista-executor'),
        })

        ballistaServerBinary.grantRead(ctx.role)
        ballistaSchedulerBinary.grantRead(ctx.role)
        ballistaExecutorBinary.grantRead(ctx.role)
    },
    onEc2Machine(ctx: OnEc2MachinesContext): void {
        const isScheduler = ctx.instanceIdx === 0;
        ctx.instanceUserData.addCommands(
            // Download pre-compiled Ballista binaries from S3
            `aws s3 cp s3://${ballistaSchedulerBinary.s3BucketName}/${ballistaSchedulerBinary.s3ObjectKey} /usr/local/bin/ballista-scheduler`,
            'chmod +x /usr/local/bin/ballista-scheduler',
            `aws s3 cp s3://${ballistaExecutorBinary.s3BucketName}/${ballistaExecutorBinary.s3ObjectKey} /usr/local/bin/ballista-executor`,
            'chmod +x /usr/local/bin/ballista-executor',
            `aws s3 cp s3://${ballistaServerBinary.s3BucketName}/${ballistaServerBinary.s3ObjectKey} /usr/local/bin/ballista-http`,
            'chmod +x /usr/local/bin/ballista-http',

            // Create Ballista directories
            'mkdir -p /var/ballista/scheduler',
            'mkdir -p /var/ballista/executor',
            'mkdir -p /var/ballista/logs',

            // Create Ballista scheduler systemd service (coordinator only)
            ...(isScheduler ? [
                `cat > /etc/systemd/system/ballista-scheduler.service << 'BALLISTA_EOF'
[Unit]
Description=Ballista Scheduler
After=network.target
[Service]
Type=simple
ExecStart=/usr/local/bin/ballista-scheduler \\
  --bind-host 0.0.0.0 \\
  --bind-port 50050
Restart=on-failure
RestartSec=5
User=root
WorkingDirectory=/var/ballista/scheduler
StandardOutput=append:/var/ballista/logs/scheduler.log
StandardError=append:/var/ballista/logs/scheduler.log
[Install]
WantedBy=multi-user.target
BALLISTA_EOF`
            ] : []),

            // Create Ballista executor systemd service (all nodes, will be reconfigured for workers)
            `cat > /etc/systemd/system/ballista-executor.service << 'BALLISTA_EOF'
[Unit]
Description=Ballista Executor
After=network.target${isScheduler ? ' ballista-scheduler.service' : ''}
${isScheduler ? 'Requires=ballista-scheduler.service' : ''}
[Service]
Type=simple
ExecStart=/usr/local/bin/ballista-executor \\
  --bind-host 0.0.0.0 \\
  --bind-port 50051 \\
  --work-dir /var/ballista/executor \\
  --scheduler-host localhost \\
  --scheduler-port 50050
Restart=on-failure
RestartSec=5
User=root
Environment="BUCKET=${ctx.bucketName}"
WorkingDirectory=/var/ballista/executor
StandardOutput=append:/var/ballista/logs/executor.log
StandardError=append:/var/ballista/logs/executor.log
[Install]
WantedBy=multi-user.target
BALLISTA_EOF`,

            // Create HTTP server systemd service (coordinator only for now)
            ...(isScheduler ? [
                `aws s3 cp s3://${ballistaServerBinary.s3BucketName}/${ballistaServerBinary.s3ObjectKey} /usr/local/bin/ballista-http`,
                'chmod +x /usr/local/bin/ballista-http',
                `cat > /etc/systemd/system/ballista-http.service << 'BALLISTA_EOF'
[Unit]
Description=Ballista HTTP Server
After=network.target ballista-scheduler.service
Requires=ballista-scheduler.service
[Service]
Type=simple
ExecStart=/usr/local/bin/ballista-http --bucket ${ctx.bucketName}
Restart=on-failure
RestartSec=5
User=root
Environment="RUST_LOG=info"
WorkingDirectory=/var/ballista
StandardOutput=append:/var/ballista/logs/http.log
StandardError=append:/var/ballista/logs/http.log
[Install]
WantedBy=multi-user.target
BALLISTA_EOF`
            ] : []),

            // Reload systemd and enable services
            'systemctl daemon-reload',

            // Enable and start scheduler (coordinator only)
            ...(isScheduler ? [
                'systemctl enable ballista-scheduler',
                'systemctl start ballista-scheduler',
                // Wait for scheduler to be ready
                'sleep 5'
            ] : []),

            // Enable and start executor (all nodes)
            'systemctl enable ballista-executor',
            'systemctl start ballista-executor',

            // Enable and start HTTP server (coordinator only)
            ...(isScheduler ? [
                'systemctl enable ballista-http',
                'systemctl start ballista-http'
            ] : [])
        )

    },
    afterEc2Machines(ctx: AfterEc2MachinesContext) {
        const [scheduler, ...executors] = ctx.instances

        // Reconfigure executors on worker nodes to point to scheduler. The executor in the machine holding the scheduler
        // communicates to it using localhost, so no need to update it with scheduler.instancePrivateIp.
        sendCommandsUnconditionally(
            ctx.scope,
            "ConfigureBallistaExecutors",
            [scheduler, ...executors],
            [
                `cat > /etc/systemd/system/ballista-executor.service << 'BALLISTA_EOF'
[Unit]
Description=Ballista Executor
After=network.target
[Service]
Type=simple
ExecStart=/usr/local/bin/ballista-executor \\
  --bind-host 0.0.0.0 \\
  --bind-port 50051 \\
  --work-dir /var/ballista/executor \\
  --scheduler-host ${scheduler.instancePrivateIp} \\
  --scheduler-port 50050
Restart=on-failure
RestartSec=5
User=root
Environment="BUCKET=${ctx.bucketName}"
WorkingDirectory=/var/ballista/executor
StandardOutput=append:/var/ballista/logs/executor.log
StandardError=append:/var/ballista/logs/executor.log
[Install]
WantedBy=multi-user.target
BALLISTA_EOF`,
                'systemctl daemon-reload',
                'systemctl restart ballista-executor',
            ]
        )
    }
}
