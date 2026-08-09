import { getStackOutput, sendSsmCommand, STACK_NAME } from "./@ops";

async function main() {
    // Step 1: Fetch stack outputs
    console.log(`Fetching outputs from stack ${STACK_NAME}...`);
    const instanceIdsStr = await getStackOutput('WorkerInstanceIds')

    if (!instanceIdsStr) {
        console.error('Error: Required outputs not found in CloudFormation stack.');
        console.error('Make sure the stack was deployed with DataFusion Distributed engine.');
        console.error(`  WorkerInstanceIds: ${instanceIdsStr ?? 'not found'}`);
        process.exit(1);
    }

    const instanceIds = instanceIdsStr.split(',');
    console.log(`Target instances: ${instanceIds.join(', ')}\n`);

    const command = process.argv.slice(2).join(" ")

    const results = await Promise.all(instanceIds.map(id => sendSsmCommand(id, [command])));

    for (let i = 0; i < instanceIds.length; i++) {
        const result = results[i];
        console.log(`\n--- ${instanceIds[i]} ---`);
        if (result.stdout) {
            console.log('STDOUT:', result.stdout);
        }
        if (result.stderr) {
            console.log('STDERR:', result.stderr);
        }
    }
}

main().catch((error) => {
    console.error('Fast deploy failed:', error);
    process.exit(1);
});
