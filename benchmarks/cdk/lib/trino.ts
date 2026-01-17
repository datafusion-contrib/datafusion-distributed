import {
    AfterEc2MachinesContext,
    QueryEngine,
    OnEc2MachinesContext,
    sendCommandsUnconditionally,
} from "./cdk-stack";

const TRINO_VERSION = 476
const JVM_MEMORY_GB = 20

export const TRINO_ENGINE: QueryEngine = {
    beforeEc2Machines(): void {
        // nothing to compile here
    },
    onEc2Machine(ctx: OnEc2MachinesContext): void {
        const isCoordinator = ctx.instanceIdx === 0;
        ctx.instanceUserData.addCommands(
            // Install Java 24 for Trino (Trino 478 requires Java 24+)
            'yum install -y java-24-amazon-corretto-headless python',

            // Download and install Trino 478 (latest version)
            'cd /opt',
            `curl -L -o trino-server.tar.gz https://repo1.maven.org/maven2/io/trino/trino-server/${TRINO_VERSION}/trino-server-${TRINO_VERSION}.tar.gz`,
            'tar -xzf trino-server.tar.gz',
            `mv trino-server-${TRINO_VERSION} trino-server`,
            'rm trino-server.tar.gz',

            // Create Trino directories
            'mkdir -p /var/trino/data',
            'mkdir -p /opt/trino-server/etc/catalog',

            // Configure Trino node properties
            `cat > /opt/trino-server/etc/node.properties << 'TRINO_EOF'
node.environment=benchmark
node.id=instance-${ctx.instanceIdx}
node.data-dir=/var/trino/data
TRINO_EOF`,

            // Configure Trino JVM settings (10GB heap to support 8GB query memory)
            `cat > /opt/trino-server/etc/jvm.config << 'TRINO_EOF'
-server
-Xmx${JVM_MEMORY_GB}G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
-Djdk.attach.allowAttachSelf=true
TRINO_EOF`,

            // Configure Trino config.properties (workers will be reconfigured during lazy startup)
            isCoordinator
                ? `cat > /opt/trino-server/etc/config.properties << 'TRINO_EOF'
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
query.max-memory-per-node=${JVM_MEMORY_GB-2}GB
memory.heap-headroom-per-node=1.5GB
TRINO_EOF`
                : `cat > /opt/trino-server/etc/config.properties << 'TRINO_EOF'
coordinator=false
http-server.http.port=8080
discovery.uri=http://localhost:8080
query.max-memory-per-node=${JVM_MEMORY_GB-2}GB
memory.heap-headroom-per-node=1.5GB
TRINO_EOF`,

            // Configure Hive catalog with AWS Glue metastore
            `cat > /opt/trino-server/etc/catalog/hive.properties << 'TRINO_EOF'
connector.name=hive
hive.metastore=glue
hive.metastore.glue.region=${ctx.region}
fs.native-s3.enabled=true
s3.region=${ctx.region}
TRINO_EOF`,

            // Configure TPCH catalog for reference
            `cat > /opt/trino-server/etc/catalog/tpch.properties << 'TRINO_EOF'
connector.name=tpch
TRINO_EOF`,

            // Download Trino CLI
            'curl -L -o /usr/local/bin/trino https://repo1.maven.org/maven2/io/trino/trino-cli/478/trino-cli-478-executable.jar',
            'chmod +x /usr/local/bin/trino',

            // Create Trino systemd service
            `cat > /etc/systemd/system/trino.service << 'TRINO_EOF'
[Unit]
Description=Trino Server
After=network.target

[Service]
Type=forking
ExecStart=/opt/trino-server/bin/launcher start
ExecStop=/opt/trino-server/bin/launcher stop
Restart=on-failure
User=root
WorkingDirectory=/opt/trino-server

[Install]
WantedBy=multi-user.target
TRINO_EOF`,

            // Enable Trino (but don't start yet - will be started lazily after all instances are up)
            'systemctl daemon-reload',
            'systemctl enable trino',
            'systemctl start trino'
        )
    },
    afterEc2Machines(ctx: AfterEc2MachinesContext): void {
        const [coordinator, ...workers] = ctx.instances
        // Then start workers (they will discover the coordinator)
        sendCommandsUnconditionally(ctx.scope, 'TrinoCoordinatorCommands',
            [coordinator],
            [
                `cat > /opt/trino-server/etc/jvm.config << 'TRINO_EOF'
-server
-Xmx${JVM_MEMORY_GB}G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
-Djdk.attach.allowAttachSelf=true
TRINO_EOF`,
                `cat > /opt/trino-server/etc/config.properties << 'TRINO_EOF'
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
query.max-memory-per-node=${JVM_MEMORY_GB-2}GB
memory.heap-headroom-per-node=1.5GB
TRINO_EOF`,
                'systemctl start trino',
            ]
        )
        sendCommandsUnconditionally(ctx.scope, 'TrinoWorkerCommands',
            workers,
            [
                `cat > /opt/trino-server/etc/config.properties << TRINO_EOF
coordinator=false
http-server.http.port=8080
discovery.uri=http://${coordinator.instancePrivateIp}:8080
query.max-memory-per-node=${JVM_MEMORY_GB-2}GB
memory.heap-headroom-per-node=1.5GB
TRINO_EOF`,
                'systemctl restart trino',
            ]
        )
    }
}