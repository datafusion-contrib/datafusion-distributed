import fs from 'node:fs';
import path from 'node:path';

const [resultDir, queryDir, requestedQueries] = process.argv.slice(2);
if (!resultDir || !queryDir) {
  throw new Error('Usage: validate-results.mjs RESULT_DIR QUERY_DIR [q1,q2,...]');
}

const queries = requestedQueries
  ? requestedQueries.split(',')
  : fs
      .readdirSync(queryDir)
      .filter((file) => /^q\d+\.sql$/.test(file))
      .map((file) => file.slice(0, -4))
      .sort((left, right) => Number(left.slice(1)) - Number(right.slice(1)));

if (queries.length === 0) {
  throw new Error('No benchmark queries were selected');
}

for (const query of queries) {
  if (!/^q\d+$/.test(query)) {
    throw new Error(`Invalid query identifier: ${query}`);
  }
  const file = path.join(resultDir, `${query}.json`);
  if (!fs.existsSync(file)) {
    throw new Error(`Missing result: ${file}`);
  }
  const result = JSON.parse(fs.readFileSync(file, 'utf8'));
  if (
    !Array.isArray(result.iterations) ||
    result.iterations.length === 0 ||
    result.iterations.some((iteration) => iteration.error)
  ) {
    throw new Error(`${query} did not complete successfully`);
  }
}

process.stdout.write(`${queries.join('\n')}\n`);
