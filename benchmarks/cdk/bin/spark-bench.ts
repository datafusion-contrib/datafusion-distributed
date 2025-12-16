import path from "path";
import {Command} from "commander";
import {ROOT, runBenchmark, BenchmarkRunner} from "./@bench-common";
import * as hive from 'hive-driver';

const { TCLIService, TCLIService_types } = hive.thrift;

// Prerequisites:
// Port-forward Spark Thrift Server: aws ssm start-session --target {instance-0-id} --document-name AWS-StartPortForwardingSession --parameters "portNumber=10000,localPortNumber=10000"

async function main() {
  const program = new Command();

  program
    .option('--sf <number>', 'Scale factor', '1')
    .option('-i, --iterations <number>', 'Number of iterations', '3')
    .option('--query <number>', 'A specific query to run', undefined)
    .option('--host <string>', 'Thrift server host', 'localhost')
    .option('--port <number>', 'Thrift server port', '10000')
    .parse(process.argv);

  const options = program.opts();

  const sf = parseInt(options.sf);
  const iterations = parseInt(options.iterations);
  const specificQuery = options.query ? parseInt(options.query) : undefined;
  const host = options.host;
  const port = parseInt(options.port);

  const runner = new SparkRunner({ sf, host, port });
  const outputPath = path.join(ROOT, "benchmarks", "data", `tpch_sf${sf}`, "spark-results.json");

  try {
    await runBenchmark(runner, {
      sf,
      iterations,
      specificQuery,
      outputPath,
    });
  } finally {
    await runner.close();
  }
}

class SparkRunner implements BenchmarkRunner {
  private client: any = null;
  private session: any = null;

  constructor(private readonly options: {
    sf: number;
    host: string;
    port: number;
  }) {
  }

  private async getSession() {
    if (!this.session) {
      const client = new hive.HiveClient(TCLIService, TCLIService_types);

      this.client = await client.connect(
        {
          host: this.options.host,
          port: this.options.port
        },
        new hive.connections.TcpConnection(),
        new hive.auth.NoSaslAuthentication()
      );

      this.session = await this.client.openSession({
        client_protocol: TCLIService_types.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10
      });

      console.log('Connected to Spark Thrift Server');
    }
    return this.session;
  }

  async close() {
    if (this.session) {
      await this.session.close();
      this.session = null;
      this.client = null;
    }
  }

  async executeQuery(sql: string): Promise<{ rowCount: number }> {
    // Handle query 15 which has multiple statements
    if (sql.includes("create view")) {
      const statements = sql.split(";").map(s => s.trim()).filter(s => s);
      const [createView, query, dropView] = statements;

      await this.executeSingleStatement(createView);
      const result = await this.executeSingleStatement(query);
      await this.executeSingleStatement(dropView);

      return result;
    } else {
      return await this.executeSingleStatement(sql);
    }
  }

  private async executeSingleStatement(sql: string): Promise<{ rowCount: number }> {
    const session = await this.getSession();

    const operation = await session.executeStatement(sql);
    await operation.finished();

    // Fetch schema and data
    let rowCount;

    const tableResult = await operation.fetchAll();
    rowCount = tableResult.length;

    await operation.close();

    return { rowCount };
  }

  async createTables(sf: number): Promise<void> {
    const database = `tpch_sf${sf}`;

    console.log(`Creating database ${database}...`);

    await this.executeSingleStatement(`CREATE DATABASE IF NOT EXISTS ${database}`);
    await this.executeSingleStatement(`USE ${database}`);

    console.log('Creating tables...');

    const tables = [
      {
        name: 'customer',
        schema: `c_custkey BIGINT, c_name STRING, c_address STRING, c_nationkey BIGINT, c_phone STRING, c_acctbal DECIMAL(15,2), c_mktsegment STRING, c_comment STRING`
      },
      {
        name: 'lineitem',
        schema: `l_orderkey BIGINT, l_partkey BIGINT, l_suppkey BIGINT, l_linenumber INT, l_quantity DECIMAL(15,2), l_extendedprice DECIMAL(15,2), l_discount DECIMAL(15,2), l_tax DECIMAL(15,2), l_returnflag STRING, l_linestatus STRING, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct STRING, l_shipmode STRING, l_comment STRING`
      },
      {
        name: 'nation',
        schema: `n_nationkey BIGINT, n_name STRING, n_regionkey BIGINT, n_comment STRING`
      },
      {
        name: 'orders',
        schema: `o_orderkey BIGINT, o_custkey BIGINT, o_orderstatus STRING, o_totalprice DECIMAL(15,2), o_orderdate DATE, o_orderpriority STRING, o_clerk STRING, o_shippriority INT, o_comment STRING`
      },
      {
        name: 'part',
        schema: `p_partkey BIGINT, p_name STRING, p_mfgr STRING, p_brand STRING, p_type STRING, p_size INT, p_container STRING, p_retailprice DECIMAL(15,2), p_comment STRING`
      },
      {
        name: 'partsupp',
        schema: `ps_partkey BIGINT, ps_suppkey BIGINT, ps_availqty INT, ps_supplycost DECIMAL(15,2), ps_comment STRING`
      },
      {
        name: 'region',
        schema: `r_regionkey BIGINT, r_name STRING, r_comment STRING`
      },
      {
        name: 'supplier',
        schema: `s_suppkey BIGINT, s_name STRING, s_address STRING, s_nationkey BIGINT, s_phone STRING, s_acctbal DECIMAL(15,2), s_comment STRING`
      }
    ];

    for (const table of tables) {
      await this.executeSingleStatement(`DROP TABLE IF EXISTS ${table.name}`);
      await this.executeSingleStatement(`CREATE EXTERNAL TABLE ${table.name} (${table.schema}) STORED AS PARQUET LOCATION 's3a://datafusion-distributed-benchmarks/tpch_sf${sf}/${table.name}/'`);
    }

    console.log('Tables created successfully!');
  }
}

main()
  .catch(err => {
    console.error(err)
    process.exit(1)
  })
