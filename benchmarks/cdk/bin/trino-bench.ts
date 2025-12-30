import path from "path";
import {Command} from "commander";
import {ROOT, runBenchmark, BenchmarkRunner, TableSpec} from "./@bench-common";

// Remember to port-forward Trino coordinator with
// aws ssm start-session --target {instance-0-id} --document-name AWS-StartPortForwardingSession --parameters "portNumber=8080,localPortNumber=8080"

async function main() {
    const program = new Command();

    program
        .option('--dataset <string>', 'Scale factor', '1')
        .option('-i, --iterations <number>', 'Number of iterations', '3')
        .option('--query <number>', 'A specific query to run', undefined)
        .parse(process.argv);

    const options = program.opts();

    const dataset: string = options.dataset
    const iterations = parseInt(options.iterations);
    const queries = options.query ? [parseInt(options.query)] : [];

    const datasetPath = path.join(ROOT, "benchmarks", "data", dataset);
    const outputPath = path.join(datasetPath, "remote-results.json")

    const runner = new TrinoRunner();

    await runBenchmark(runner, {
        dataset,
        iterations,
        queries,
        outputPath,
    });
}

class TrinoRunner implements BenchmarkRunner {
    private trinoUrl = 'http://localhost:8080';
    private schema?: string

    async executeQuery(sql: string): Promise<{ rowCount: number }> {
        // Fix TPCH query 4: Add DATE prefix to date literals that don't have it.
        sql = sql.replace(/(?<!date\s)('[\d]{4}-[\d]{2}-[\d]{2}')/gi, 'DATE $1');

        // Fix TPCH query 15: Trino doesn't support column list in CREATE VIEW, need to use aliases in SELECT
        sql = sql.replace(
            /create view revenue0 \(supplier_no, total_revenue\) as\s+select\s+l_suppkey,\s+sum\(l_extendedprice \* \(1 - l_discount\)\)/is,
            'create view revenue0 as select l_suppkey as supplier_no, sum(l_extendedprice * (1 - l_discount)) as total_revenue'
        );

        // Fix ClickBench queries: Convert to_timestamp_seconds to Trino's from_unixtime
        sql = sql.replace(/to_timestamp_seconds\(/gi, 'from_unixtime(');

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
        if (!this.schema) {
            throw new Error("No schema available, where the tables created?")
        }

        // Submit query
        const submitResponse = await fetch(`${this.trinoUrl}/v1/statement`, {
            method: 'POST',
            headers: {
                'X-Trino-User': 'benchmark',
                'X-Trino-Catalog': 'hive',
                'X-Trino-Schema': this.schema ?? '',
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

    async createTables(tables: TableSpec[]): Promise<void> {
        if (tables.length === 0) {
            throw new Error("No table passed")
        }
        let schema = tables[0].schema
        let basePath = tables[0].s3Path.split('/').slice(0, -1).join("/")

        this.schema = schema

        await this.executeSingleStatement(`
            CREATE SCHEMA IF NOT EXISTS hive."${schema}" WITH (location = '${basePath}')`);

        for (const table of tables) {
            await this.executeSingleStatement(`
                DROP TABLE IF EXISTS hive."${table.schema}"."${table.name}"`);

            await this.executeSingleStatement(` 
                CREATE TABLE hive."${table.schema}"."${table.name}" ${getSchema(table)} 
                WITH (external_location = '${table.s3Path}', format = 'PARQUET')`);
        }
    }
}

const SCHEMAS: Record<string, Record<string, string>> = {
    tpch: {
        customer: `(
   c_custkey    bigint,
   c_name       varchar(25),
   c_address    varchar(40),
   c_nationkey  bigint,
   c_phone      varchar(15),
   c_acctbal    decimal(15, 2),
   c_mktsegment varchar(10),
   c_comment    varchar(117)
)`,
        lineitem: `(
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
)`,
        nation: `(
   n_nationkey bigint,
   n_name      varchar(25),
   n_regionkey bigint,
   n_comment   varchar(152)
)`,
        orders: `(
   o_orderkey      bigint,
   o_custkey       bigint,
   o_orderstatus   varchar(1),
   o_totalprice    decimal(15, 2),
   o_orderdate     date,
   o_orderpriority varchar(15),
   o_clerk         varchar(15),
   o_shippriority  integer,
   o_comment       varchar(79)
)`,
        part: `(
   p_partkey     bigint,
   p_name        varchar(55),
   p_mfgr        varchar(25),
   p_brand       varchar(10),
   p_type        varchar(25),
   p_size        integer,
   p_container   varchar(10),
   p_retailprice decimal(15, 2),
   p_comment     varchar(23)
)`,
        partsupp: `(
   ps_partkey    bigint,
   ps_suppkey    bigint,
   ps_availqty   integer,
   ps_supplycost decimal(15, 2),
   ps_comment    varchar(199)
)`,
        region: `(
   r_regionkey bigint,
   r_name      varchar(25),
   r_comment   varchar(152)
)`,
        supplier: `(
   s_suppkey   bigint,
   s_name      varchar(25),
   s_address   varchar(40),
   s_nationkey bigint,
   s_phone     varchar(15),
   s_acctbal   decimal(15, 2),
   s_comment   varchar(101)
)`
    },
    clickbench: {
        hits: `(
   WatchID bigint,
   JavaEnable smallint,
   Title varchar,
   GoodEvent smallint,
   EventTime bigint,
   EventDate date,
   CounterID integer,
   ClientIP integer,
   RegionID integer,
   UserID bigint,
   CounterClass smallint,
   OS smallint,
   UserAgent smallint,
   URL varchar,
   Referer varchar,
   IsRefresh smallint,
   RefererCategoryID smallint,
   RefererRegionID integer,
   URLCategoryID smallint,
   URLRegionID integer,
   ResolutionWidth smallint,
   ResolutionHeight smallint,
   ResolutionDepth smallint,
   FlashMajor smallint,
   FlashMinor smallint,
   FlashMinor2 varchar,
   NetMajor smallint,
   NetMinor smallint,
   UserAgentMajor smallint,
   UserAgentMinor varchar(255),
   CookieEnable smallint,
   JavascriptEnable smallint,
   IsMobile smallint,
   MobilePhone smallint,
   MobilePhoneModel varchar,
   Params varchar,
   IPNetworkID integer,
   TraficSourceID smallint,
   SearchEngineID smallint,
   SearchPhrase varchar,
   AdvEngineID smallint,
   IsArtifical smallint,
   WindowClientWidth smallint,
   WindowClientHeight smallint,
   ClientTimeZone smallint,
   ClientEventTime bigint,
   SilverlightVersion1 smallint,
   SilverlightVersion2 smallint,
   SilverlightVersion3 integer,
   SilverlightVersion4 smallint,
   PageCharset varchar,
   CodeVersion integer,
   IsLink smallint,
   IsDownload smallint,
   IsNotBounce smallint,
   FUniqID bigint,
   OriginalURL varchar,
   HID integer,
   IsOldCounter smallint,
   IsEvent smallint,
   IsParameter smallint,
   DontCountHits smallint,
   WithHash smallint,
   HitColor varchar(1),
   LocalEventTime bigint,
   Age smallint,
   Sex smallint,
   Income smallint,
   Interests smallint,
   Robotness smallint,
   RemoteIP integer,
   WindowName integer,
   OpenerName integer,
   HistoryLength smallint,
   BrowserLanguage varchar,
   BrowserCountry varchar,
   SocialNetwork varchar,
   SocialAction varchar,
   HTTPError smallint,
   SendTiming integer,
   DNSTiming integer,
   ConnectTiming integer,
   ResponseStartTiming integer,
   ResponseEndTiming integer,
   FetchTiming integer,
   SocialSourceNetworkID smallint,
   SocialSourcePage varchar,
   ParamPrice bigint,
   ParamOrderID varchar,
   ParamCurrency varchar,
   ParamCurrencyID smallint,
   OpenstatServiceName varchar,
   OpenstatCampaignID varchar,
   OpenstatAdID varchar,
   OpenstatSourceID varchar,
   UTMSource varchar,
   UTMMedium varchar,
   UTMCampaign varchar,
   UTMContent varchar,
   UTMTerm varchar,
   FromTag varchar,
   HasGCLID smallint,
   RefererHash bigint,
   URLHash bigint,
   CLID integer
)`
    }
}

function getSchema(table: TableSpec): string {
    const tableSchema = SCHEMAS[table.schema.split("_")[0]]?.[table.name]
    if (!tableSchema) {
        throw new Error(`Could not find table ${table.name} in schema ${table.schema}`)
    }
    return tableSchema
}

main()
    .catch(err => {
        console.error(err)
        process.exit(1)
    })
