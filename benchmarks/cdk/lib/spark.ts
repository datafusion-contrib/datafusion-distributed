import * as ec2 from 'aws-cdk-lib/aws-ec2';

const SPARK_VERSION = '4.0.1'
const HADOOP_VERSION = '3'

export function sparkUserDataCommands(instanceIndex: number, region: string): string[] {
  const isMaster = instanceIndex === 0;

  return [
    // Install Java 17 for Spark (Java 24 is already installed for Trino)
    'yum install -y java-17-amazon-corretto-headless python',

    // Download and install Spark
    'cd /opt',
    `curl -L -o spark.tgz https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz`,
    'tar -xzf spark.tgz',
    `mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark`,
    'rm spark.tgz',

    // Create Spark directories
    'mkdir -p /var/spark/logs',
    'mkdir -p /var/spark/work',
    'mkdir -p /var/spark/tmp',

    // Download AWS SDK JARs for S3 access
    'cd /opt/spark/jars',
    'curl -L -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar',
    'curl -L -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar',

    // Set JAVA_HOME and SPARK_HOME
    `cat > /opt/spark/conf/spark-env.sh << 'SPARK_EOF'
export JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64
export SPARK_HOME=/opt/spark
export SPARK_LOG_DIR=/var/spark/logs
export SPARK_WORKER_DIR=/var/spark/work
export SPARK_LOCAL_DIRS=/var/spark/tmp
export SPARK_MASTER_HOST=localhost
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
export SPARK_WORKER_WEBUI_PORT=8081
export SPARK_WORKER_MEMORY=8g
export SPARK_WORKER_CORES=4
SPARK_EOF`,
    'chmod +x /opt/spark/conf/spark-env.sh',

    // Configure Spark defaults
    `cat > /opt/spark/conf/spark-defaults.conf << 'SPARK_EOF'
spark.master spark://localhost:7077
spark.eventLog.enabled true
spark.eventLog.dir /var/spark/logs
spark.history.fs.logDirectory /var/spark/logs
spark.sql.warehouse.dir /var/spark/warehouse
spark.driver.memory 8g
spark.executor.memory 8g
spark.sql.adaptive.enabled true
spark.sql.adaptive.coalescePartitions.enabled true
spark.hadoop.fs.s3a.impl org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider com.amazonaws.auth.InstanceProfileCredentialsProvider
SPARK_EOF`,

    // Create Spark Master systemd service
    isMaster
      ? `cat > /etc/systemd/system/spark-master.service << 'SPARK_EOF'
[Unit]
Description=Spark Master
After=network.target

[Service]
Type=forking
ExecStart=/opt/spark/sbin/start-master.sh
ExecStop=/opt/spark/sbin/stop-master.sh
Restart=on-failure
User=root
WorkingDirectory=/opt/spark
Environment="JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64"
Environment="SPARK_HOME=/opt/spark"

[Install]
WantedBy=multi-user.target
SPARK_EOF`
      : 'echo "Worker node - master service not created"',

    // Create Spark Worker systemd service
    `cat > /etc/systemd/system/spark-worker.service << 'SPARK_EOF'
[Unit]
Description=Spark Worker
After=network.target

[Service]
Type=forking
ExecStart=/opt/spark/sbin/start-worker.sh spark://localhost:7077
ExecStop=/opt/spark/sbin/stop-worker.sh
Restart=on-failure
User=root
WorkingDirectory=/opt/spark
Environment="JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64"
Environment="SPARK_HOME=/opt/spark"

[Install]
WantedBy=multi-user.target
SPARK_EOF`,

    // Enable services (but don't start yet - will be started lazily after all instances are up)
    'systemctl daemon-reload',
    ...(isMaster
      ? [
          'systemctl enable spark-master',
          'systemctl enable spark-worker'
        ]
      : [
          'systemctl enable spark-worker'
        ])
  ];
}

export function sparkMasterCommands() {
  return [
    'systemctl start spark-master',
    'sleep 5',  // Wait for master to start
    'systemctl start spark-worker',  // Master also runs a worker
  ];
}

export function sparkWorkerCommands(master: ec2.Instance) {
  return [
    `cat > /opt/spark/conf/spark-env.sh << SPARK_EOF
export JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64
export SPARK_HOME=/opt/spark
export SPARK_LOG_DIR=/var/spark/logs
export SPARK_WORKER_DIR=/var/spark/work
export SPARK_LOCAL_DIRS=/var/spark/tmp
export SPARK_MASTER_HOST=${master.instancePrivateIp}
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
export SPARK_WORKER_WEBUI_PORT=8081
export SPARK_WORKER_MEMORY=8g
export SPARK_WORKER_CORES=4
SPARK_EOF`,
    `cat > /opt/spark/conf/spark-defaults.conf << SPARK_EOF
spark.master spark://${master.instancePrivateIp}:7077
spark.eventLog.enabled true
spark.eventLog.dir /var/spark/logs
spark.history.fs.logDirectory /var/spark/logs
spark.sql.warehouse.dir /var/spark/warehouse
spark.driver.memory 8g
spark.executor.memory 8g
spark.sql.adaptive.enabled true
spark.sql.adaptive.coalescePartitions.enabled true
spark.hadoop.fs.s3a.impl org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider com.amazonaws.auth.InstanceProfileCredentialsProvider
SPARK_EOF`,
    'systemctl start spark-worker',
  ];
}
