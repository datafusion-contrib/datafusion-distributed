import path from "path";
import { Command } from "commander";
import { ROOT, runBenchmark, BenchmarkRunner } from "./@bench-common";

// Remember to port-forward Trino coordinator with
// aws ssm start-session --target {instance-0-id} --document-name AWS-StartPortForwardingSession --parameters "portNumber=8080,localPortNumber=8080"

async function main() {
  const program = new Command();

  program
    .option('--sf <number>', 'Scale factor', '1')
    .option('-i, --iterations <number>', 'Number of iterations', '3')
    .option('--query <number>', 'A specific query to run', undefined)
    .parse(process.argv);

  const options = program.opts();

  const sf = parseInt(options.sf);
  const iterations = parseInt(options.iterations);
  const specificQuery = options.query ? parseInt(options.query) : undefined;

  const runner = new TrinoRunner({ sf });
  const outputPath = path.join(ROOT, "benchmarks", "data", `tpch_sf${sf}`, "remote-results.json");

  await runBenchmark(runner, {
    sf,
    iterations,
    specificQuery,
    outputPath,
  });
}

class TrinoRunner implements BenchmarkRunner {
  private trinoUrl = 'http://localhost:8080';

  constructor(private readonly options: {
    sf: number
  }) {
  }


  async executeQuery(sql: string): Promise<{ rowCount: number }> {
    // Fix query 4: Add DATE prefix to date literals that don't have it.
    sql = sql.replace(/(?<!date\s)('[\d]{4}-[\d]{2}-[\d]{2}')/gi, 'DATE $1');

    // Fix query 15: Trino doesn't support column list in CREATE VIEW, need to use aliases in SELECT
    sql = sql.replace(
      /create view revenue0 \(supplier_no, total_revenue\) as\s+select\s+l_suppkey,\s+sum\(l_extendedprice \* \(1 - l_discount\)\)/is,
      'create view revenue0 as select l_suppkey as supplier_no, sum(l_extendedprice * (1 - l_discount)) as total_revenue'
    );

    let response
    if (sql.includes("create view")) {
      // This is query 15
      let [createView, query, dropView] = sql.split(";")
      await this.executeSingleStatement(createView);
      response = await this.executeSingleStatement(`EXPLAIN ANALYZE ${query}`); // Use EXPLAIN ANALYZE for the actual query
      await this.executeSingleStatement(dropView);
    } else {
      response = await this.executeSingleStatement(`EXPLAIN ANALYZE ${sql}`)
    }

    return response
  }

  private async executeSingleStatement(sql: string): Promise<{ rowCount: number }> {
    // Submit query
    const submitResponse = await fetch(`${this.trinoUrl}/v1/statement`, {
      method: 'POST',
      headers: {
        'X-Trino-User': 'benchmark',
        'X-Trino-Catalog': 'hive',
        'X-Trino-Schema': `tpch_sf${this.options.sf}`,
      },
      body: sql.trim().replace(/;+$/, ''),
    });

    if (!submitResponse.ok) {
      const msg = await submitResponse.text();
      throw new Error(`Query submission failed: ${submitResponse.status} ${msg}`);
    }

    let result: any = await submitResponse.json();
    let rowCount = 0;

    // Poll for results
    while (result.nextUri) {
      const pollResponse = await fetch(result.nextUri);

      if (!pollResponse.ok) {
        const msg = await pollResponse.text();
        throw new Error(`Query polling failed: ${pollResponse.status} ${msg}`);
      }

      result = await pollResponse.json();

      // Count rows if data is present
      if (result.data) {
        if (typeof result.data?.[0]?.[0] === 'string') {
          // Extract row count from EXPLAIN ANALYZE output
          const outputMatch = result.data[0][0].match(/Output.*?(\d+)\s+rows/i);
          if (outputMatch) {
            rowCount = parseInt(outputMatch[1]);
          }
        } else {
          rowCount += result.data.length;
        }
      }

      // Check for errors
      if (result.error) {
        throw new Error(`Query failed: ${result.error.message}`);
      }
    }

    return { rowCount };
  }

  async createTables(sf: number): Promise<void> {
    const schema = `tpch_sf${sf}`;

    // Create schema first
    await this.executeSingleStatement(`CREATE SCHEMA IF NOT EXISTS hive.${schema} WITH (location = 's3://datafusion-distributed-benchmarks/tpch_sf${sf}/')`);

    // Create customer table
    await this.executeSingleStatement(`DROP TABLE IF EXISTS hive.${schema}.customer`);
    await this.executeSingleStatement(`CREATE TABLE hive.${schema}.customer
                                       (
                                           c_custkey    bigint,
                                           c_name       varchar(25),
                                           c_address    varchar(40),
                                           c_nationkey  bigint,
                                           c_phone      varchar(15),
                                           c_acctbal    decimal(15, 2),
                                           c_mktsegment varchar(10),
                                           c_comment    varchar(117)
                                       )
        WITH (external_location = 's3://datafusion-distributed-benchmarks/tpch_sf${sf}/customer/', format = 'PARQUET')`);

    // Create lineitem table
    await this.executeSingleStatement(`DROP TABLE IF EXISTS hive.${schema}.lineitem`);
    await this.executeSingleStatement(`CREATE TABLE hive.${schema}.lineitem
                                       (
                                           l_orderkey      bigint,
                                           l_partkey       bigint,
                                           l_suppkey       bigint,
                                           l_linenumber    integer,
                                           l_quantity      decimal(15, 2),
                                           l_extendedprice decimal(15, 2),
                                           l_discount      decimal(15, 2),
                                           l_tax           decimal(15, 2),
                                           l_returnflag    varchar(1),
                                           l_linestatus    varchar(1),
                                           l_shipdate      date,
                                           l_commitdate    date,
                                           l_receiptdate   date,
                                           l_shipinstruct  varchar(25),
                                           l_shipmode      varchar(10),
                                           l_comment       varchar(44)
                                       )
        WITH (external_location = 's3://datafusion-distributed-benchmarks/tpch_sf${sf}/lineitem/', format = 'PARQUET')`);

    // Create nation table
    await this.executeSingleStatement(`DROP TABLE IF EXISTS hive.${schema}.nation`);
    await this.executeSingleStatement(`CREATE TABLE hive.${schema}.nation
                                       (
                                           n_nationkey bigint,
                                           n_name      varchar(25),
                                           n_regionkey bigint,
                                           n_comment   varchar(152)
                                       )
        WITH (external_location = 's3://datafusion-distributed-benchmarks/tpch_sf${sf}/nation/', format = 'PARQUET')`);

    // Create orders table
    await this.executeSingleStatement(`DROP TABLE IF EXISTS hive.${schema}.orders`);
    await this.executeSingleStatement(`CREATE TABLE hive.${schema}.orders
                                       (
                                           o_orderkey      bigint,
                                           o_custkey       bigint,
                                           o_orderstatus   varchar(1),
                                           o_totalprice    decimal(15, 2),
                                           o_orderdate     date,
                                           o_orderpriority varchar(15),
                                           o_clerk         varchar(15),
                                           o_shippriority  integer,
                                           o_comment       varchar(79)
                                       )
        WITH (external_location = 's3://datafusion-distributed-benchmarks/tpch_sf${sf}/orders/', format = 'PARQUET')`);

    // Create part table
    await this.executeSingleStatement(`DROP TABLE IF EXISTS hive.${schema}.part`);
    await this.executeSingleStatement(`CREATE TABLE hive.${schema}.part
                                       (
                                           p_partkey     bigint,
                                           p_name        varchar(55),
                                           p_mfgr        varchar(25),
                                           p_brand       varchar(10),
                                           p_type        varchar(25),
                                           p_size        integer,
                                           p_container   varchar(10),
                                           p_retailprice decimal(15, 2),
                                           p_comment     varchar(23)
                                       )
        WITH (external_location = 's3://datafusion-distributed-benchmarks/tpch_sf${sf}/part/', format = 'PARQUET')`);

    // Create partsupp table
    await this.executeSingleStatement(`DROP TABLE IF EXISTS hive.${schema}.partsupp`);
    await this.executeSingleStatement(`CREATE TABLE hive.${schema}.partsupp
                                       (
                                           ps_partkey    bigint,
                                           ps_suppkey    bigint,
                                           ps_availqty   integer,
                                           ps_supplycost decimal(15, 2),
                                           ps_comment    varchar(199)
                                       )
        WITH (external_location = 's3://datafusion-distributed-benchmarks/tpch_sf${sf}/partsupp/', format = 'PARQUET')`);

    // Create region table
    await this.executeSingleStatement(`DROP TABLE IF EXISTS hive.${schema}.region`);
    await this.executeSingleStatement(`CREATE TABLE hive.${schema}.region
                                       (
                                           r_regionkey bigint,
                                           r_name      varchar(25),
                                           r_comment   varchar(152)
                                       )
        WITH (external_location = 's3://datafusion-distributed-benchmarks/tpch_sf${sf}/region/', format = 'PARQUET')`);

    // Create supplier table
    await this.executeSingleStatement(`DROP TABLE IF EXISTS hive.${schema}.supplier`);
    await this.executeSingleStatement(`CREATE TABLE hive.${schema}.supplier
                                       (
                                           s_suppkey   bigint,
                                           s_name      varchar(25),
                                           s_address   varchar(40),
                                           s_nationkey bigint,
                                           s_phone     varchar(15),
                                           s_acctbal   decimal(15, 2),
                                           s_comment   varchar(101)
                                       )
        WITH (external_location = 's3://datafusion-distributed-benchmarks/tpch_sf${sf}/supplier/', format = 'PARQUET')`);
  }
}

main()
  .catch(err => {
    console.error(err)
    process.exit(1)
  })
