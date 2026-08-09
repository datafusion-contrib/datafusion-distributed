import { exec } from 'child_process';
import { promisify } from 'util';

const execAsync = promisify(exec);

export const STACK_NAME = 'DataFusionDistributedBenchmarks';

export async function getStackOutput(outputKey: string): Promise<string | undefined> {
    try {
        const { stdout } = await execAsync(
            `aws cloudformation describe-stacks --stack-name ${STACK_NAME} --query "Stacks[0].Outputs[?OutputKey=='${outputKey}'].OutputValue" --output text`
        );
        const value = stdout.trim();
        return value && value !== 'None' ? value : undefined;
    } catch {
        return undefined;
    }
}

function sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

export interface SsmCommandResult {
    success: boolean;
    stdout: string;
    stderr: string;
}

async function waitForCommand(commandId: string, instanceId: string): Promise<SsmCommandResult> {
    const maxAttempts = 60;
    const pollInterval = 2000;
    const SUCCESS = 'Success'
    const FAILED = 'Failed'
    const CANCELLED = 'Cancelled'
    const TIMED_OUT = 'TimedOut'

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
        try {
            const { stdout } = await execAsync(
                `aws ssm get-command-invocation --command-id "${commandId}" --instance-id "${instanceId}" --output json`
            );
            const result = JSON.parse(stdout);
            const status = result.Status;

            if (status === SUCCESS) {
                return {
                    success: true,
                    stdout: result.StandardOutputContent || '',
                    stderr: result.StandardErrorContent || '',
                };
            } else if (status === FAILED || status === CANCELLED || status === TIMED_OUT) {
                console.error(`  ${instanceId}: Command ${status} - ${result.StatusDetails}`);
                return {
                    success: false,
                    stdout: result.StandardOutputContent || '',
                    stderr: result.StandardErrorContent || '',
                };
            }
        } catch {
            // Command invocation might not be ready yet, wait and retry
        }
        await sleep(pollInterval);
    }

    console.error(`  ${instanceId}: Timed out waiting for command`);
    return { success: false, stdout: '', stderr: '' };
}

export async function sendSsmCommand(instanceId: string, commands: string[]): Promise<SsmCommandResult> {
    console.log(`Sending commands to ${instanceId}...`);
    try {
        const { stdout } = await execAsync(
            `aws ssm send-command --instance-ids "${instanceId}" --document-name "AWS-RunShellScript" --parameters '{"commands":${JSON.stringify(commands)}}' --query "Command.CommandId" --output text`
        );
        const commandId = stdout.trim();
        console.log(`  ${instanceId}: Command ID ${commandId}, waiting for completion...`);

        const result = await waitForCommand(commandId, instanceId);
        if (result.success) {
            console.log(`  ${instanceId}: Success`);
        }
        return result;
    } catch (error) {
        console.error(`  ${instanceId}: Failed to send command:`, error);
        return { success: false, stdout: '', stderr: String(error) };
    }
}

