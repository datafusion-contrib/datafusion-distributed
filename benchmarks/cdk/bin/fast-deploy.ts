import { execSync } from 'child_process';
import * as path from 'path';
import { getStackOutput, sendSsmCommand, STACK_NAME } from "./@ops";

const ROOT = path.join(__dirname, '..', '..', '..');
const WORKER_BINARY_PATH = path.join(ROOT, 'target/x86_64-unknown-linux-gnu/release/worker');

async function main() {
    // Step 1: Build the worker binary
    console.log('Building worker binary...');
    execSync('cargo zigbuild -p datafusion-distributed-benchmarks --release --bin worker --target x86_64-unknown-linux-gnu', {
        cwd: ROOT,
        stdio: 'inherit',
        env: { ...process.env, FORCE_REBUILD: Date.now().toString() },
    });
    console.log('Worker binary built successfully.\n');

    // Step 2: Fetch stack outputs (in parallel)
    console.log(`Fetching outputs from stack ${STACK_NAME}...`);
    const [instanceIdsStr, s3Bucket, s3Key] = await Promise.all([
        getStackOutput('WorkerInstanceIds'),
        getStackOutput('WorkerBinaryS3Bucket'),
        getStackOutput('WorkerBinaryS3Key'),
    ]);

    if (!instanceIdsStr || !s3Bucket || !s3Key) {
        console.error('Error: Required outputs not found in CloudFormation stack.');
        console.error('Make sure the stack was deployed with DataFusion Distributed engine.');
        console.error(`  WorkerInstanceIds: ${instanceIdsStr ?? 'not found'}`);
        console.error(`  WorkerBinaryS3Bucket: ${s3Bucket ?? 'not found'}`);
        console.error(`  WorkerBinaryS3Key: ${s3Key ?? 'not found'}`);
        process.exit(1);
    }

    const instanceIds = instanceIdsStr.split(',');
    console.log(`Target instances: ${instanceIds.join(', ')}\n`);

    // Step 3: Upload the binary to S3
    console.log(`Uploading worker binary to s3://${s3Bucket}/${s3Key}...`);
    execSync(`aws s3 cp ${WORKER_BINARY_PATH} s3://${s3Bucket}/${s3Key}`, { stdio: 'inherit' });
    console.log('Upload complete.\n');

    // Step 4: Send SSM commands to all instances (in parallel)
    const commands = [
        `aws s3 cp s3://${s3Bucket}/${s3Key} /usr/local/bin/worker`,
        'chmod +x /usr/local/bin/worker',
        'systemctl restart worker',
    ];

    const results = await Promise.all(instanceIds.map(id => sendSsmCommand(id, commands)));
    const successCount = results.filter(r => r).length;
    const failCount = results.length - successCount;

    console.log(`\nFast deploy complete: ${successCount} succeeded, ${failCount} failed.`);
    if (failCount > 0) {
        process.exit(1);
    }
}

main().catch((error) => {
    console.error('Fast deploy failed:', error);
    process.exit(1);
});
