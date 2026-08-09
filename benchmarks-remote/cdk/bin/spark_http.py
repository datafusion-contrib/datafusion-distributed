#!/usr/bin/env python3
import os
import time
from flask import Flask, request, jsonify
from pyspark.sql import SparkSession

app = Flask(__name__)

# Initialize Spark session
spark = None

def get_spark():
    global spark
    if spark is None:
        master_host = os.environ.get('SPARK_MASTER_HOST', 'localhost')
        spark_jars = os.environ.get('SPARK_JARS', '/opt/spark/jars/hadoop-aws-3.4.1.jar,/opt/spark/jars/bundle-2.29.52.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar')
        spark = SparkSession.builder \
            .appName("SparkHTTPServer") \
            .master(f"spark://{master_host}:7077") \
            .config("spark.jars", spark_jars) \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
            .enableHiveSupport() \
            .getOrCreate()

        # Set log level to reduce noise
        spark.sparkContext.setLogLevel("WARN")
    return spark

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy"}), 200

@app.route('/query', methods=['POST'])
def execute_query():
    """Execute a SQL query on Spark"""
    try:
        data = request.get_json()
        if not data or 'query' not in data:
            return jsonify({"error": "Missing 'query' in request body"}), 400

        query = data['query']

        # Execute the query
        spark_session = get_spark()
        start = time.monotonic()
        df = spark_session.sql(query)

        # Get row count without collecting all data
        count = df.count()
        elapsed_ms = (time.monotonic() - start) * 1000

        return jsonify({"count": count, "elapsed_ms": elapsed_ms}), 200

    except Exception as e:
        return str(e), 500

if __name__ == '__main__':
    # Run Flask server on port 9000
    port = int(os.environ.get('HTTP_PORT', 9003))
    app.run(host='0.0.0.0', port=port, debug=False)
