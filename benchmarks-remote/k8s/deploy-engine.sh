#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
engine=${1:?usage: deploy-engine.sh ENGINE}
region=${AWS_REGION:-us-east-1}
outputs_file=${PULUMI_OUTPUTS_FILE:-${root}/benchmarks-remote/pulumi/.pulumi-outputs.json}
runtime_file=${K8S_RUNTIME_FILE:-${root}/benchmarks-remote/k8s/.runtime.json}

case ${engine} in
  datafusion | trino | spark | ballista) ;;
  *)
    echo "Unknown benchmark engine '${engine}'" >&2
    exit 2
    ;;
esac

if [[ ! -f ${outputs_file} ]]; then
  echo "Missing ${outputs_file}; run npm run foundation-deploy first" >&2
  exit 2
fi

output_value() {
  local expression=$1
  local file=$2
  if [[ -f ${file} ]]; then
    jq -r "${expression} // empty" "${file}"
  fi
}

cluster_name=$(jq -er '.clusterName' "${outputs_file}")
dataset_bucket=$(jq -er '.datasetBucketName' "${outputs_file}")
benchmark_instance_type=$(jq -er '.benchmarkInstanceType' "${outputs_file}")
coordinator_instance_type=$(jq -er '.coordinatorInstanceType' "${outputs_file}")
node_count=${NODE_COUNT:-$(jq -er '.benchmarkNodeCount' "${outputs_file}")}
source "${root}/benchmarks-remote/k8s/lib.sh"
require_aws_credentials
ensure_kubeconfig

manifest_values=()
case ${engine} in
  datafusion)
    "${root}/benchmarks-remote/k8s/publish-datafusion.sh"
    worker_artifact=${WORKER_ARTIFACT:-$(output_value '.workerArtifact' "${runtime_file}")}
    : "${worker_artifact:?DataFusion worker publishing did not produce an artifact}"
    manifest_values+=(--set-string worker.artifact="${worker_artifact}")
    manifest_values+=(--set-string worker.datasetBucket="${dataset_bucket}")
    manifest_values+=(--set-string worker.replicas="${node_count}")
    manifest_values+=(--set-string worker.instanceType="${benchmark_instance_type}")
    ;;
  trino)
    manifest_values+=(--set-string region="${region}")
    manifest_values+=(--set-string datasetBucket="${dataset_bucket}")
    manifest_values+=(--set-string workerReplicas="${node_count}")
    manifest_values+=(--set-string workerInstanceType="${benchmark_instance_type}")
    manifest_values+=(--set-string coordinatorInstanceType="${coordinator_instance_type}")
    ;;
  spark)
    "${root}/benchmarks-remote/k8s/publish-image.sh" spark
    spark_image=${SPARK_IMAGE:-$(output_value '.images.spark' "${runtime_file}")}
    : "${spark_image:?Spark publishing did not produce an image}"
    manifest_values+=(--set-string image="${spark_image}")
    manifest_values+=(--set-string workerReplicas="${node_count}")
    manifest_values+=(--set-string workerInstanceType="${benchmark_instance_type}")
    manifest_values+=(--set-string coordinatorInstanceType="${coordinator_instance_type}")
    ;;
  ballista)
    "${root}/benchmarks-remote/k8s/publish-ballista.sh"
    scheduler_artifact=$(output_value '.ballistaArtifacts["ballista-scheduler"]' "${runtime_file}")
    executor_artifact=$(output_value '.ballistaArtifacts["ballista-executor"]' "${runtime_file}")
    http_artifact=$(output_value '.ballistaArtifacts["ballista-http"]' "${runtime_file}")
    : "${scheduler_artifact:?Ballista publishing did not produce the scheduler artifact}"
    : "${executor_artifact:?Ballista publishing did not produce the executor artifact}"
    : "${http_artifact:?Ballista publishing did not produce the HTTP artifact}"
    manifest_values+=(--set-string datasetBucket="${dataset_bucket}")
    manifest_values+=(--set-string artifacts.scheduler="${scheduler_artifact}")
    manifest_values+=(--set-string artifacts.executor="${executor_artifact}")
    manifest_values+=(--set-string artifacts.http="${http_artifact}")
    manifest_values+=(--set-string workerReplicas="${node_count}")
    manifest_values+=(--set-string workerInstanceType="${benchmark_instance_type}")
    manifest_values+=(--set-string coordinatorInstanceType="${coordinator_instance_type}")
    ;;
esac

HELM_CACHE_HOME=/tmp/datafusion-distributed-helm-cache \
  HELM_CONFIG_HOME=/tmp/datafusion-distributed-helm-config \
  HELM_DATA_HOME=/tmp/datafusion-distributed-helm-data \
  helm upgrade --install "${engine}" "${root}/benchmarks-remote/k8s/${engine}" \
  --namespace "benchmark-${engine}" \
  --kube-context "${cluster_name}" \
  --values "${root}/benchmarks-remote/k8s/worker-resources.yaml" \
  --rollback-on-failure \
  --cleanup-on-fail \
  --wait \
  --timeout 25m \
  "${manifest_values[@]}"

echo "Deployed ${engine}; it will remain running until npm run ${engine}-destroy"
