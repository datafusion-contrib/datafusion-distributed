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

const SPARK_VERSION = "4.0.0"
const HADOOP_VERSION = "3"

let sparkHttpScript: s3assets.Asset
let sparkRequirements: s3assets.Asset

export const SPARK_ENGINE: QueryEngine = {
    beforeEc2Machines(ctx: BeforeEc2MachinesContext): void {
        // Upload Python HTTP server script to S3
        sparkHttpScript = new s3assets.Asset(ctx.scope, 'SparkHttpScript', {
            path: path.join(ROOT, 'benchmarks/cdk/bin/spark_http.py'),
        })

        // Upload Python requirements file to S3
        sparkRequirements = new s3assets.Asset(ctx.scope, 'SparkRequirements', {
            path: path.join(ROOT, 'benchmarks/cdk/requirements.txt'),
        })

        sparkHttpScript.grantRead(ctx.role)
        sparkRequirements.grantRead(ctx.role)
    },
    onEc2Machine(ctx: OnEc2MachinesContext): void {
        const isMaster = ctx.instanceIdx === 0;
        ctx.instanceUserData.addCommands(
            // Install Java 17 for Spark
            'yum install -y java-17-amazon-corretto-headless python3 python3-pip',

            // Download and install Spark
            'cd /opt',
            `curl -L -o spark.tgz https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz`,
            'tar -xzf spark.tgz',
            `mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark`,
            'rm spark.tgz',

            // Download AWS JARs for S3 access (Hadoop 3.4.1 with AWS SDK V2 for Spark 4.0)
            'cd /opt/spark/jars',
            'curl -L -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar',
            'curl -L -O https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar',
            'curl -L -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar',

            // Create Spark directories
            'mkdir -p /var/spark/logs',
            'mkdir -p /var/spark/work',
            'mkdir -p /var/spark/http',

            // Download Python scripts from S3
            `aws s3 cp s3://${sparkHttpScript.s3BucketName}/${sparkHttpScript.s3ObjectKey} /var/spark/http/spark_http.py`,
            'chmod +x /var/spark/http/spark_http.py',
            `aws s3 cp s3://${sparkRequirements.s3BucketName}/${sparkRequirements.s3ObjectKey} /var/spark/http/requirements.txt`,

            // Install Python dependencies
            'pip3 install -r /var/spark/http/requirements.txt',

            // Configure Spark defaults
            `cat > /opt/spark/conf/spark-defaults.conf << 'SPARK_EOF'
spark.master spark://localhost:7077
spark.executor.memory 4g
spark.driver.memory 2g
spark.sql.warehouse.dir /var/spark/warehouse
spark.jars /opt/spark/jars/hadoop-aws-3.4.1.jar,/opt/spark/jars/bundle-2.29.52.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar
spark.hadoop.fs.s3a.impl org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider com.amazonaws.auth.InstanceProfileCredentialsProvider
spark.hadoop.fs.s3a.connection.timeout 60000
spark.hadoop.fs.s3a.connection.establish.timeout 60000
spark.hadoop.fs.s3a.attempts.maximum 10
spark.sql.catalogImplementation hive
spark.hadoop.hive.metastore.client.factory.class com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory
spark.sql.hive.metastore.version 2.3.9
spark.sql.hive.metastore.jars builtin
SPARK_EOF`,

            // Configure Hadoop core-site.xml for S3A with numeric timeouts
            `cat > /opt/spark/conf/core-site.xml << 'CORE_SITE_EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.s3a.impl</name>
    <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
  </property>
  <property>
    <name>fs.s3a.aws.credentials.provider</name>
    <value>com.amazonaws.auth.InstanceProfileCredentialsProvider</value>
  </property>
  <property>
    <name>fs.s3a.connection.timeout</name>
    <value>60000</value>
  </property>
  <property>
    <name>fs.s3a.connection.establish.timeout</name>
    <value>60000</value>
  </property>
  <property>
    <name>fs.s3a.connection.request.timeout</name>
    <value>60000</value>
  </property>
  <property>
    <name>fs.s3a.attempts.maximum</name>
    <value>10</value>
  </property>
  <property>
    <name>fs.s3a.retry.interval</name>
    <value>500</value>
  </property>
  <property>
    <name>fs.s3a.retry.limit</name>
    <value>10</value>
  </property>
</configuration>
CORE_SITE_EOF`,

            // Configure Spark environment
            `cat > /opt/spark/conf/spark-env.sh << 'SPARK_EOF'
#!/usr/bin/env bash
export SPARK_WORKER_DIR=/var/spark/work
export SPARK_LOG_DIR=/var/spark/logs
export SPARK_MASTER_HOST=localhost
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8082
export SPARK_WORKER_WEBUI_PORT=8083
export JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64
SPARK_EOF`,
            'chmod +x /opt/spark/conf/spark-env.sh',

            // Create Spark master systemd service (master only)
            ...(isMaster ? [
                `cat > /etc/systemd/system/spark-master.service << 'SPARK_EOF'
[Unit]
Description=Spark Master
After=network.target

[Service]
Type=forking
ExecStart=/opt/spark/sbin/start-master.sh
ExecStop=/opt/spark/sbin/stop-master.sh
Restart=on-failure
RestartSec=5
User=root
WorkingDirectory=/opt/spark
Environment="JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64"
Environment="SPARK_LOG_DIR=/var/spark/logs"

[Install]
WantedBy=multi-user.target
SPARK_EOF`
            ] : []),

            // Create Spark worker systemd service (will be reconfigured for workers)
            `cat > /etc/systemd/system/spark-worker.service << 'SPARK_EOF'
[Unit]
Description=Spark Worker
After=network.target${isMaster ? ' spark-master.service' : ''}
${isMaster ? 'Requires=spark-master.service' : ''}

[Service]
Type=forking
ExecStart=/opt/spark/sbin/start-worker.sh spark://localhost:7077
ExecStop=/opt/spark/sbin/stop-worker.sh
Restart=on-failure
RestartSec=5
User=root
WorkingDirectory=/opt/spark
Environment="JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64"
Environment="SPARK_LOG_DIR=/var/spark/logs"
Environment="SPARK_WORKER_DIR=/var/spark/work"

[Install]
WantedBy=multi-user.target
SPARK_EOF`,

            // Create HTTP server systemd service (master only)
            ...(isMaster ? [
                `cat > /etc/systemd/system/spark-http.service << 'SPARK_EOF'
[Unit]
Description=Spark HTTP Server
After=network.target spark-master.service
Requires=spark-master.service

[Service]
Type=simple
ExecStart=/usr/bin/python3 /var/spark/http/spark_http.py
Restart=on-failure
RestartSec=5
User=root
WorkingDirectory=/var/spark/http
Environment="SPARK_MASTER_HOST=localhost"
Environment="HTTP_PORT=9003"
Environment="JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64"
Environment="SPARK_JARS=/opt/spark/jars/hadoop-aws-3.4.1.jar,/opt/spark/jars/bundle-2.29.52.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
StandardOutput=append:/var/spark/logs/http.log
StandardError=append:/var/spark/logs/http.log

[Install]
WantedBy=multi-user.target
SPARK_EOF`
            ] : []),

            // Reload systemd and enable services
            'systemctl daemon-reload',

            // Enable and start master (master only)
            ...(isMaster ? [
                'systemctl enable spark-master',
                'systemctl start spark-master',
                'sleep 5'
            ] : []),

            // Enable and start worker (all nodes)
            'systemctl enable spark-worker',
            'systemctl start spark-worker',

            // Enable and start HTTP server (master only)
            ...(isMaster ? [
                'systemctl enable spark-http',
                'systemctl start spark-http'
            ] : [])
        )
    },
    afterEc2Machines(ctx: AfterEc2MachinesContext): void {
        const [master, ...workers] = ctx.instances

        // Reconfigure all workers to point to master IP
        sendCommandsUnconditionally(
            ctx.scope,
            'ConfigureSparkWorkers',
            workers,
            [
                `cat > /opt/spark/conf/spark-env.sh << 'SPARK_EOF'
#!/usr/bin/env bash
export SPARK_WORKER_DIR=/var/spark/work
export SPARK_LOG_DIR=/var/spark/logs
export SPARK_MASTER_HOST=${master.instancePrivateIp}
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8082
export SPARK_WORKER_WEBUI_PORT=8081
export JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64
SPARK_EOF`,
                'chmod +x /opt/spark/conf/spark-env.sh',
                `cat > /etc/systemd/system/spark-worker.service << 'SPARK_EOF'
[Unit]
Description=Spark Worker
After=network.target

[Service]
Type=forking
ExecStart=/opt/spark/sbin/start-worker.sh spark://${master.instancePrivateIp}:7077
ExecStop=/opt/spark/sbin/stop-worker.sh
Restart=on-failure
RestartSec=5
User=root
WorkingDirectory=/opt/spark
Environment="JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64"
Environment="SPARK_LOG_DIR=/var/spark/logs"
Environment="SPARK_WORKER_DIR=/var/spark/work"

[Install]
WantedBy=multi-user.target
SPARK_EOF`,
                'systemctl daemon-reload',
                'systemctl restart spark-worker'
            ]
        )

        // Also update master's spark-env.sh to use its own IP for proper cluster formation
        sendCommandsUnconditionally(
            ctx.scope,
            'ConfigureSparkMaster',
            [master],
            [
                `cat > /opt/spark/conf/spark-env.sh << 'SPARK_EOF'
#!/usr/bin/env bash
export SPARK_WORKER_DIR=/var/spark/work
export SPARK_LOG_DIR=/var/spark/logs
export SPARK_MASTER_HOST=${master.instancePrivateIp}
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8082
export SPARK_WORKER_WEBUI_PORT=8081
export JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64
SPARK_EOF`,
                'chmod +x /opt/spark/conf/spark-env.sh',
                `cat > /etc/systemd/system/spark-worker.service << 'SPARK_EOF'
[Unit]
Description=Spark Worker
After=network.target spark-master.service
Requires=spark-master.service

[Service]
Type=forking
ExecStart=/opt/spark/sbin/start-worker.sh spark://${master.instancePrivateIp}:7077
ExecStop=/opt/spark/sbin/stop-worker.sh
Restart=on-failure
RestartSec=5
User=root
WorkingDirectory=/opt/spark
Environment="JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64"
Environment="SPARK_LOG_DIR=/var/spark/logs"
Environment="SPARK_WORKER_DIR=/var/spark/work"

[Install]
WantedBy=multi-user.target
SPARK_EOF`,
                `cat > /etc/systemd/system/spark-http.service << 'SPARK_EOF'
[Unit]
Description=Spark HTTP Server
After=network.target spark-master.service
Requires=spark-master.service

[Service]
Type=simple
ExecStart=/usr/bin/python3 /var/spark/http/spark_http.py
Restart=on-failure
RestartSec=5
User=root
WorkingDirectory=/var/spark/http
Environment="SPARK_MASTER_HOST=${master.instancePrivateIp}"
Environment="HTTP_PORT=9003"
Environment="JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64"
Environment="SPARK_JARS=/opt/spark/jars/hadoop-aws-3.4.1.jar,/opt/spark/jars/bundle-2.29.52.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
StandardOutput=append:/var/spark/logs/http.log
StandardError=append:/var/spark/logs/http.log

[Install]
WantedBy=multi-user.target
SPARK_EOF`,
                'systemctl daemon-reload',
                'systemctl restart spark-master',
                'systemctl restart spark-worker',
                'systemctl restart spark-http'
            ]
        )
    }
}
