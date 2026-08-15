#!/usr/bin/env bash

set -e

# Join Order Benchmark IMDB snapshot used by DataFusion and the JOB paper.
# https://event.cwi.nl/da/job/imdb.tgz
IMDB_URL=${IMDB_URL:-"https://event.cwi.nl/da/job/imdb.tgz"}
EXPECTED_SIZE=${EXPECTED_SIZE:-1263193115}

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
DATA_DIR=${DATA_DIR:-${REPO_ROOT}/testdata/imdb}
CARGO_COMMAND=${CARGO_COMMAND:-"cargo run -p datafusion-distributed-benchmarks --release"}

TABLES=(
  aka_name aka_title cast_info char_name comp_cast_type company_name
  company_type complete_cast info_type keyword kind_type link_type
  movie_companies movie_info movie_info_idx movie_keyword movie_link
  name person_info role_type title
)

echo "Preparing IMDB (JOB) dataset in ${DATA_DIR}"
mkdir -p "${DATA_DIR}"

all_parquet=true
for table in "${TABLES[@]}"; do
  if [ ! -f "${DATA_DIR}/${table}/0.parquet" ]; then
    all_parquet=false
    break
  fi
done

if [ "${all_parquet}" = true ]; then
  echo " parquet tables already exist in ${DATA_DIR}."
  exit 0
fi

missing_csv=false
for table in "${TABLES[@]}"; do
  if [ ! -f "${DATA_DIR}/${table}.csv" ]; then
    missing_csv=true
    break
  fi
done

if [ "${missing_csv}" = true ]; then
  TGZ="${DATA_DIR}/imdb.tgz"
  echo -n "Looking for imdb.tgz... "
  if [ -f "${TGZ}" ]; then
    echo "found"
    OUTPUT_SIZE=$(wc -c "${TGZ}" 2>/dev/null | awk '{print $1}' || true)
    if [ "${OUTPUT_SIZE}" != "${EXPECTED_SIZE}" ]; then
      echo "Size mismatch: ${OUTPUT_SIZE} found, ${EXPECTED_SIZE} expected. Re-downloading..."
      rm -f "${TGZ}"
    fi
  else
    echo "not found"
  fi

  if [ ! -f "${TGZ}" ]; then
    echo "Downloading IMDB dataset from ${IMDB_URL} (~1.2 GB)..."
    curl -L -o "${TGZ}" "${IMDB_URL}"
  fi

  echo "Extracting ${TGZ}..."
  tar -xzf "${TGZ}" -C "${DATA_DIR}"
fi

echo "Converting IMDB CSVs to parquet..."
$CARGO_COMMAND -- prepare-imdb --input "${DATA_DIR}" --output "${DATA_DIR}"
echo "IMDB dataset ready. Run with: ./benchmarks/run.sh --dataset imdb"
