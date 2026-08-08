#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
region=${AWS_REGION:-us-east-1}
outputs_file=${PULUMI_OUTPUTS_FILE:-${root}/benchmarks-remote/pulumi/.pulumi-outputs.json}
runtime_file=${K8S_RUNTIME_FILE:-${root}/benchmarks-remote/k8s/.runtime.json}

output_value() {
  local expression=$1
  local file=$2
  if [[ -f ${file} ]]; then
    jq -r "${expression} // empty" "${file}"
  fi
}

server_instance_id=${K3S_SERVER_INSTANCE_ID:-$(output_value '.k3sServerInstanceId' "${outputs_file}")}
datafusion_asg=${DATAFUSION_ASG:-$(output_value '.engineNodeGroupNames.datafusion' "${outputs_file}")}
dataset_bucket=${DATASET_BUCKET:-$(output_value '.datasetBucketName' "${outputs_file}")}
results_bucket=${RESULTS_BUCKET:-$(output_value '.resultsBucketName' "${outputs_file}")}
worker_image=${WORKER_IMAGE:-$(output_value '.workerImage' "${runtime_file}")}
: "${server_instance_id:?set K3S_SERVER_INSTANCE_ID or generate ${outputs_file}}"
: "${datafusion_asg:?set DATAFUSION_ASG or generate ${outputs_file}}"
: "${dataset_bucket:?set DATASET_BUCKET or generate ${outputs_file}}"
: "${results_bucket:?set RESULTS_BUCKET or generate ${outputs_file}}"
: "${worker_image:?set WORKER_IMAGE or run npm run datafusion-deploy:k8s}"
source "${root}/benchmarks-remote/k8s/lib.sh"
node_count=${NODE_COUNT:-2}
local_port=${LOCAL_PORT:-9000}
run_id=${RUN_ID:-$(date -u +%Y%m%dt%H%M%sz)}
bootstrap_prefix="s3://${results_bucket}/runs/bootstrap"
run_prefix="s3://${results_bucket}/runs/${run_id}"
lock_acquired=false
tunnel_pid=
tunnel_log=

dataset=
queries=
arguments=("$@")
for ((index = 0; index < ${#arguments[@]}; index++)); do
  case ${arguments[index]} in
    --dataset)
      ((index += 1))
      dataset=${arguments[index]:-}
      ;;
    --dataset=*) dataset=${arguments[index]#--dataset=} ;;
    --queries)
      ((index += 1))
      queries=${arguments[index]:-}
      ;;
    --queries=*) queries=${arguments[index]#--queries=} ;;
  esac
done
if [[ -z ${dataset} ]]; then
  echo "Pass the benchmark dataset, for example: --dataset tpch_sf1" >&2
  exit 2
fi
dataset_suite=${dataset%%_*}
dataset_variant=${dataset#*_}
if [[ ${dataset} == "${dataset_suite}" ]]; then
  dataset_directory=benchmark
elif [[ ${dataset_suite} == clickbench ]]; then
  dataset_directory="benchmark_range${dataset_variant}"
else
  dataset_directory="benchmark_${dataset_variant}"
fi
local_dataset_path="${root}/testdata/${dataset_suite}/${dataset_directory}"
if [[ ! -d ${local_dataset_path} ]]; then
  echo "Local dataset directory does not exist: ${local_dataset_path}" >&2
  exit 2
fi

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM
  set +e
  if [[ -n ${tunnel_pid} ]]; then
    kill "${tunnel_pid}" 2>/dev/null
    wait "${tunnel_pid}" 2>/dev/null
  fi
  if ${lock_acquired}; then
    ssm_run "k3s kubectl delete daemonset datafusion-worker -n benchmark-datafusion --ignore-not-found" 600
    aws_cli autoscaling update-auto-scaling-group \
      --auto-scaling-group-name "${datafusion_asg}" \
      --desired-capacity 0
    for attempt in $(seq 1 90); do
      instance_count=$(aws_cli autoscaling describe-auto-scaling-groups \
        --auto-scaling-group-names "${datafusion_asg}" \
        --query 'length(AutoScalingGroups[0].Instances)' \
        --output text)
      if [[ ${instance_count} == 0 ]]; then
        break
      fi
      sleep 10
    done
    ssm_run "nodes=\$(k3s kubectl get nodes -l benchmark.datafusion.apache.org/engine=datafusion --no-headers 2>/dev/null | awk '\$2 == \"NotReady\" {print \$1}'); if [ -n \"\${nodes}\" ]; then k3s kubectl delete node \${nodes}; fi" 300
    ssm_run "k3s kubectl delete configmap benchmark-run-lock -n benchmark-system --ignore-not-found" 300
  fi
  if [[ -n ${tunnel_log} ]]; then
    rm -f "${tunnel_log}"
  fi
  exit "${exit_code}"
}
trap cleanup EXIT INT TERM
tunnel_log=$(mktemp)

ssm_run "k3s kubectl create configmap benchmark-run-lock -n benchmark-system --from-literal=run-id=${run_id}"
lock_acquired=true

aws_cli s3 sync "${local_dataset_path}" "s3://${dataset_bucket}/${dataset}" \
  --exclude '*' \
  --include '*/part-*.parquet'

registry=${worker_image%%/*}
ssm_run "password=\$(aws ecr get-login-password --region ${region}); k3s kubectl create secret docker-registry benchmark-ecr --namespace=benchmark-datafusion --docker-server=${registry} --docker-username=AWS --docker-password=\"\${password}\" --dry-run=client -o yaml | k3s kubectl apply -f -; unset password"

aws_cli autoscaling update-auto-scaling-group \
  --auto-scaling-group-name "${datafusion_asg}" \
  --desired-capacity "${node_count}"

ssm_run "for attempt in \$(seq 1 90); do ready=\$(k3s kubectl get nodes -l benchmark.datafusion.apache.org/engine=datafusion --no-headers 2>/dev/null | awk '\$2 == \"Ready\" {count++} END {print count+0}'); if [ \"\${ready}\" -eq ${node_count} ]; then k3s kubectl get nodes -l benchmark.datafusion.apache.org/engine=datafusion -o wide; exit 0; fi; sleep 10; done; exit 1" 1000

worker_manifest=$(mktemp)
HELM_CACHE_HOME=/tmp/datafusion-distributed-helm-cache \
  HELM_CONFIG_HOME=/tmp/datafusion-distributed-helm-config \
  HELM_DATA_HOME=/tmp/datafusion-distributed-helm-data \
  helm template datafusion "${root}/benchmarks-remote/k8s/datafusion" \
  --set-string worker.image="${worker_image}" \
  --set-string worker.datasetBucket="${dataset_bucket}" >"${worker_manifest}"
aws_cli s3 cp "${worker_manifest}" "${bootstrap_prefix}/datafusion-worker-${run_id}.yaml"
rm -f "${worker_manifest}"
ssm_run "aws s3 cp ${bootstrap_prefix}/datafusion-worker-${run_id}.yaml /tmp/datafusion-worker.yaml; k3s kubectl apply -f /tmp/datafusion-worker.yaml; k3s kubectl rollout status daemonset/datafusion-worker -n benchmark-datafusion --timeout=10m" 900

coordinator_instance_id=$(aws_cli autoscaling describe-auto-scaling-groups \
  --auto-scaling-group-names "${datafusion_asg}" \
  --query "AutoScalingGroups[0].Instances[?LifecycleState=='InService'] | [0].InstanceId" \
  --output text)
if [[ -z ${coordinator_instance_id} || ${coordinator_instance_id} == None ]]; then
  echo "Could not select a DataFusion coordinator instance" >&2
  exit 1
fi
for attempt in $(seq 1 60); do
  ssm_status=$(aws_cli ssm describe-instance-information \
    --filters "Key=InstanceIds,Values=${coordinator_instance_id}" \
    --query 'InstanceInformationList[0].PingStatus' \
    --output text)
  if [[ ${ssm_status} == Online ]]; then
    break
  fi
  if [[ ${attempt} -eq 60 ]]; then
    echo "Timed out waiting for SSM on coordinator ${coordinator_instance_id}" >&2
    exit 1
  fi
  sleep 5
done

aws_cli ssm start-session \
  --target "${coordinator_instance_id}" \
  --document-name AWS-StartPortForwardingSession \
  --parameters "portNumber=9000,localPortNumber=${local_port}" >"${tunnel_log}" 2>&1 &
tunnel_pid=$!
for attempt in $(seq 1 30); do
  if curl --silent --show-error --fail "http://127.0.0.1:${local_port}/info" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "${tunnel_pid}" 2>/dev/null; then
    cat "${tunnel_log}" >&2
    exit 1
  fi
  if [[ ${attempt} -eq 30 ]]; then
    cat "${tunnel_log}" >&2
    echo "Timed out waiting for the local SSM tunnel" >&2
    exit 1
  fi
  sleep 2
done

branch=$(git -C "${root}" rev-parse --abbrev-ref HEAD | awk -F/ '{print $NF}')
engine="datafusion-distributed-${BENCHMARK_ENGINE:-${branch}}"
(
  cd "${root}/benchmarks-remote/cdk"
  DATAFUSION_URL="http://127.0.0.1:${local_port}" \
    BENCHMARK_BUCKET="s3://${dataset_bucket}" \
    BENCHMARK_ENGINE="${BENCHMARK_ENGINE:-${branch}}" \
    npm run datafusion-bench -- "${arguments[@]}"
)

result_dir="${local_dataset_path}/.results-remote/${engine}"
query_dir="${root}/testdata/${dataset%%_*}/queries"
validated_queries=$(node "${root}/benchmarks-remote/k8s/validate-results.mjs" \
  "${result_dir}" "${query_dir}" "${queries}")
result_count=$(awk 'NF { count++ } END { print count+0 }' <<<"${validated_queries}")

aws_cli s3 cp \
  "${local_dataset_path}/previous-remote.json" \
  "${run_prefix}/previous-remote.json"
result_includes=(--exclude '*')
while IFS= read -r query; do
  result_includes+=(--include "${query}.json")
done <<<"${validated_queries}"
aws_cli s3 cp --recursive \
  "${result_dir}" \
  "${run_prefix}/results/${engine}" \
  "${result_includes[@]}"

persisted_count=$(aws_cli s3api list-objects-v2 \
  --bucket "${results_bucket}" \
  --prefix "runs/${run_id}/results/${engine}/" \
  --query 'length(Contents)' \
  --output text)
if [[ ${persisted_count} != "${result_count}" ]]; then
  echo "Expected ${result_count} persisted query results, found ${persisted_count}" >&2
  exit 1
fi
echo "Benchmark run completed locally and persisted: ${run_prefix}"
