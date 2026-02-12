import { Command } from "commander";
import { BenchmarkRunner, ExecuteQueryResult, runBenchmark, TableSpec } from "./@bench-common";

// Remember to port-forward Trino coordinator with
// aws ssm start-session --target {instance-0-id} --document-name AWS-StartPortForwardingSession --parameters "portNumber=8080,localPortNumber=8080"

async function main() {
    const program = new Command();

    program
        .requiredOption('--dataset <string>', 'Scale factor', '1')
        .option('-i, --iterations <number>', 'Number of iterations', '3')
        .option('--queries <string>', 'Specific queries to run', undefined)
        .option('--debug <boolean>', 'Print the generated plans to stdout')
        .option('--warmup <boolean>', 'Perform a warmup query before the benchmarks')
        .parse(process.argv);

    const options = program.opts();

    const dataset: string = options.dataset
    const iterations = parseInt(options.iterations);
    const queries = options.queries?.split(",") ?? []
    const debug = options.debug === 'true' || options.debug === 1
    const warmup = options.warmup === 'true' || options.debug === 1

    const runner = new TrinoRunner();

    await runBenchmark(runner, {
        dataset,
        engine: 'trino',
        iterations,
        queries,
        debug,
        warmup
    });
}

class TrinoRunner implements BenchmarkRunner {
    private trinoUrl = 'http://localhost:8080';
    private schema?: string

    async executeQuery(sql: string): Promise<ExecuteQueryResult> {
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

        // Extract total server-side latency from the EXPLAIN ANALYZE plan header.
        // The plan contains: "Queued: 9.38ms, Analysis: 38.95ms, Planning: 200.19ms, Execution: 573.65ms"
        // The stats JSON fields (wallTimeMillis, etc.) are aggregates across all tasks/splits
        // and don't represent the per-query timing, so the plan header is the reliable source.
        const timingMatches = [...response.plan.matchAll(/(Queued|Analysis|Planning|Execution):\s+([\d.]+)(us|ms|s|m)/g)];
        if (timingMatches.length === 0) {
            throw new Error(`Could not extract timing from EXPLAIN ANALYZE plan:\n${response.plan}`);
        }
        let elapsed = 0;
        for (const match of timingMatches) {
            const value = parseFloat(match[2]);
            const unit = match[3];
            elapsed += unit === 'us' ? value / 1000 : unit === 's' ? value * 1000 : unit === 'm' ? value * 60000 : value;
        }

        return { ...response, elapsed }
    }

    private async executeSingleStatement(sql: string): Promise<{ rowCount: number, plan: string }> {
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
        let plan = "";

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
                    plan = result.data[0][0]
                    // Extract row count from EXPLAIN ANALYZE output
                    const outputMatch = plan.match(/Output.*?(\d+)\s+rows/i);
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

        return { rowCount, plan };
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
    },
    tpcds: {
        call_center: `(
   cc_call_center_sk integer,
   cc_call_center_id varchar,
   cc_rec_start_date date,
   cc_rec_end_date date,
   cc_closed_date_sk double,
   cc_open_date_sk integer,
   cc_name varchar,
   cc_class varchar,
   cc_employees integer,
   cc_sq_ft integer,
   cc_hours varchar,
   cc_manager varchar,
   cc_mkt_id integer,
   cc_mkt_class varchar,
   cc_mkt_desc varchar,
   cc_market_manager varchar,
   cc_division integer,
   cc_division_name varchar,
   cc_company integer,
   cc_company_name varchar,
   cc_street_number varchar,
   cc_street_name varchar,
   cc_street_type varchar,
   cc_suite_number varchar,
   cc_city varchar,
   cc_county varchar,
   cc_state varchar,
   cc_zip varchar,
   cc_country varchar,
   cc_gmt_offset decimal(3, 2),
   cc_tax_percentage decimal(2, 2)
)`,
        catalog_page: `(
   cp_catalog_page_sk integer,
   cp_catalog_page_id varchar,
   cp_start_date_sk double,
   cp_end_date_sk double,
   cp_department varchar,
   cp_catalog_number double,
   cp_catalog_page_number double,
   cp_description varchar,
   cp_type varchar
)`,
        catalog_returns: `(
   cr_returned_date_sk integer,
   cr_returned_time_sk integer,
   cr_item_sk integer,
   cr_refunded_customer_sk double,
   cr_refunded_cdemo_sk double,
   cr_refunded_hdemo_sk double,
   cr_refunded_addr_sk double,
   cr_returning_customer_sk double,
   cr_returning_cdemo_sk double,
   cr_returning_hdemo_sk double,
   cr_returning_addr_sk double,
   cr_call_center_sk double,
   cr_catalog_page_sk double,
   cr_ship_mode_sk double,
   cr_warehouse_sk double,
   cr_reason_sk double,
   cr_order_number integer,
   cr_return_quantity double,
   cr_return_amount decimal(7, 2),
   cr_return_tax decimal(6, 2),
   cr_return_amt_inc_tax decimal(7, 2),
   cr_fee decimal(5, 2),
   cr_return_ship_cost decimal(7, 2),
   cr_refunded_cash decimal(7, 2),
   cr_reversed_charge decimal(7, 2),
   cr_store_credit decimal(7, 2),
   cr_net_loss decimal(7, 2)
)`,
        catalog_sales: `(
   cs_sold_date_sk double,
   cs_sold_time_sk double,
   cs_ship_date_sk double,
   cs_bill_customer_sk double,
   cs_bill_cdemo_sk double,
   cs_bill_hdemo_sk double,
   cs_bill_addr_sk double,
   cs_ship_customer_sk double,
   cs_ship_cdemo_sk double,
   cs_ship_hdemo_sk double,
   cs_ship_addr_sk double,
   cs_call_center_sk double,
   cs_catalog_page_sk double,
   cs_ship_mode_sk double,
   cs_warehouse_sk double,
   cs_item_sk integer,
   cs_promo_sk double,
   cs_order_number integer,
   cs_quantity double,
   cs_wholesale_cost decimal(5, 2),
   cs_list_price decimal(5, 2),
   cs_sales_price decimal(5, 2),
   cs_ext_discount_amt decimal(7, 2),
   cs_ext_sales_price decimal(7, 2),
   cs_ext_wholesale_cost decimal(7, 2),
   cs_ext_list_price decimal(7, 2),
   cs_ext_tax decimal(6, 2),
   cs_coupon_amt decimal(7, 2),
   cs_ext_ship_cost decimal(7, 2),
   cs_net_paid decimal(7, 2),
   cs_net_paid_inc_tax decimal(7, 2),
   cs_net_paid_inc_ship decimal(7, 2),
   cs_net_paid_inc_ship_tax decimal(7, 2),
   cs_net_profit decimal(7, 2)
)`,
        customer: `(
   c_customer_sk integer,
   c_customer_id varchar,
   c_current_cdemo_sk double,
   c_current_hdemo_sk double,
   c_current_addr_sk integer,
   c_first_shipto_date_sk double,
   c_first_sales_date_sk double,
   c_salutation varchar,
   c_first_name varchar,
   c_last_name varchar,
   c_preferred_cust_flag varchar,
   c_birth_day double,
   c_birth_month double,
   c_birth_year double,
   c_birth_country varchar,
   c_login varchar,
   c_email_address varchar,
   c_last_review_date double
)`,
        customer_address: `(
   ca_address_sk integer,
   ca_address_id varchar,
   ca_street_number varchar,
   ca_street_name varchar,
   ca_street_type varchar,
   ca_suite_number varchar,
   ca_city varchar,
   ca_county varchar,
   ca_state varchar,
   ca_zip varchar,
   ca_country varchar,
   ca_gmt_offset decimal(4, 2),
   ca_location_type varchar
)`,
        customer_demographics: `(
   cd_demo_sk integer,
   cd_gender varchar,
   cd_marital_status varchar,
   cd_education_status varchar,
   cd_purchase_estimate integer,
   cd_credit_rating varchar,
   cd_dep_count integer,
   cd_dep_employed_count integer,
   cd_dep_college_count integer
)`,
        date_dim: `(
   d_date_sk integer,
   d_date_id varchar,
   d_date date,
   d_month_seq integer,
   d_week_seq integer,
   d_quarter_seq integer,
   d_year integer,
   d_dow integer,
   d_moy integer,
   d_dom integer,
   d_qoy integer,
   d_fy_year integer,
   d_fy_quarter_seq integer,
   d_fy_week_seq integer,
   d_day_name varchar,
   d_quarter_name varchar,
   d_holiday varchar,
   d_weekend varchar,
   d_following_holiday varchar,
   d_first_dom integer,
   d_last_dom integer,
   d_same_day_ly integer,
   d_same_day_lq integer,
   d_current_day varchar,
   d_current_week varchar,
   d_current_month varchar,
   d_current_quarter varchar,
   d_current_year varchar
)`,
        household_demographics: `(
   hd_demo_sk integer,
   hd_income_band_sk integer,
   hd_buy_potential varchar,
   hd_dep_count integer,
   hd_vehicle_count integer
)`,
        income_band: `(
   ib_income_band_sk integer,
   ib_lower_bound integer,
   ib_upper_bound integer
)`,
        inventory: `(
   inv_date_sk integer,
   inv_item_sk integer,
   inv_warehouse_sk integer,
   inv_quantity_on_hand double
)`,
        item: `(
   i_item_sk integer,
   i_item_id varchar,
   i_rec_start_date date,
   i_rec_end_date date,
   i_item_desc varchar,
   i_current_price decimal(4, 2),
   i_wholesale_cost decimal(4, 2),
   i_brand_id double,
   i_brand varchar,
   i_class_id double,
   i_class varchar,
   i_category_id double,
   i_category varchar,
   i_manufact_id double,
   i_manufact varchar,
   i_size varchar,
   i_formulation varchar,
   i_color varchar,
   i_units varchar,
   i_container varchar,
   i_manager_id double,
   i_product_name varchar
)`,
        promotion: `(
   p_promo_sk integer,
   p_promo_id varchar,
   p_start_date_sk double,
   p_end_date_sk double,
   p_item_sk double,
   p_cost decimal(6, 2),
   p_response_target double,
   p_promo_name varchar,
   p_channel_dmail varchar,
   p_channel_email varchar,
   p_channel_catalog varchar,
   p_channel_tv varchar,
   p_channel_radio varchar,
   p_channel_press varchar,
   p_channel_event varchar,
   p_channel_demo varchar,
   p_channel_details varchar,
   p_purpose varchar,
   p_discount_active varchar
)`,
        reason: `(
   r_reason_sk integer,
   r_reason_id varchar,
   r_reason_desc varchar
)`,
        ship_mode: `(
   sm_ship_mode_sk integer,
   sm_ship_mode_id varchar,
   sm_type varchar,
   sm_code varchar,
   sm_carrier varchar,
   sm_contract varchar
)`,
        store: `(
   s_store_sk integer,
   s_store_id varchar,
   s_rec_start_date date,
   s_rec_end_date date,
   s_closed_date_sk double,
   s_store_name varchar,
   s_number_employees integer,
   s_floor_space integer,
   s_hours varchar,
   s_manager varchar,
   s_market_id integer,
   s_geography_class varchar,
   s_market_desc varchar,
   s_market_manager varchar,
   s_division_id integer,
   s_division_name varchar,
   s_company_id integer,
   s_company_name varchar,
   s_street_number varchar,
   s_street_name varchar,
   s_street_type varchar,
   s_suite_number varchar,
   s_city varchar,
   s_county varchar,
   s_state varchar,
   s_zip varchar,
   s_country varchar,
   s_gmt_offset decimal(3, 2),
   s_tax_precentage decimal(2, 2)
)`,
        store_returns: `(
   sr_returned_date_sk double,
   sr_return_time_sk double,
   sr_item_sk integer,
   sr_customer_sk double,
   sr_cdemo_sk double,
   sr_hdemo_sk double,
   sr_addr_sk double,
   sr_store_sk double,
   sr_reason_sk double,
   sr_ticket_number integer,
   sr_return_quantity double,
   sr_return_amt decimal(7, 2),
   sr_return_tax decimal(6, 2),
   sr_return_amt_inc_tax decimal(7, 2),
   sr_fee decimal(5, 2),
   sr_return_ship_cost decimal(6, 2),
   sr_refunded_cash decimal(7, 2),
   sr_reversed_charge decimal(7, 2),
   sr_store_credit decimal(7, 2),
   sr_net_loss decimal(6, 2)
)`,
        store_sales: `(
   ss_sold_date_sk double,
   ss_sold_time_sk double,
   ss_item_sk integer,
   ss_customer_sk double,
   ss_cdemo_sk double,
   ss_hdemo_sk double,
   ss_addr_sk double,
   ss_store_sk double,
   ss_promo_sk double,
   ss_ticket_number integer,
   ss_quantity double,
   ss_wholesale_cost decimal(5, 2),
   ss_list_price decimal(5, 2),
   ss_sales_price decimal(5, 2),
   ss_ext_discount_amt decimal(7, 2),
   ss_ext_sales_price decimal(7, 2),
   ss_ext_wholesale_cost decimal(7, 2),
   ss_ext_list_price decimal(7, 2),
   ss_ext_tax decimal(6, 2),
   ss_coupon_amt decimal(7, 2),
   ss_net_paid decimal(7, 2),
   ss_net_paid_inc_tax decimal(7, 2),
   ss_net_profit decimal(6, 2)
)`,
        time_dim: `(
   t_time_sk integer,
   t_time_id varchar,
   t_time integer,
   t_hour integer,
   t_minute integer,
   t_second integer,
   t_am_pm varchar,
   t_shift varchar,
   t_sub_shift varchar,
   t_meal_time varchar
)`,
        warehouse: `(
   w_warehouse_sk integer,
   w_warehouse_id varchar,
   w_warehouse_name varchar,
   w_warehouse_sq_ft integer,
   w_street_number varchar,
   w_street_name varchar,
   w_street_type varchar,
   w_suite_number varchar,
   w_city varchar,
   w_county varchar,
   w_state varchar,
   w_zip varchar,
   w_country varchar,
   w_gmt_offset decimal(3, 2)
)`,
        web_page: `(
   wp_web_page_sk integer,
   wp_web_page_id varchar,
   wp_rec_start_date date,
   wp_rec_end_date date,
   wp_creation_date_sk integer,
   wp_access_date_sk integer,
   wp_autogen_flag varchar,
   wp_customer_sk double,
   wp_url varchar,
   wp_type varchar,
   wp_char_count integer,
   wp_link_count integer,
   wp_image_count integer,
   wp_max_ad_count integer
)`,
        web_returns: `(
   wr_returned_date_sk double,
   wr_returned_time_sk double,
   wr_item_sk integer,
   wr_refunded_customer_sk double,
   wr_refunded_cdemo_sk double,
   wr_refunded_hdemo_sk double,
   wr_refunded_addr_sk double,
   wr_returning_customer_sk double,
   wr_returning_cdemo_sk double,
   wr_returning_hdemo_sk double,
   wr_returning_addr_sk double,
   wr_web_page_sk double,
   wr_reason_sk double,
   wr_order_number integer,
   wr_return_quantity double,
   wr_return_amt decimal(7, 2),
   wr_return_tax decimal(6, 2),
   wr_return_amt_inc_tax decimal(7, 2),
   wr_fee decimal(5, 2),
   wr_return_ship_cost decimal(7, 2),
   wr_refunded_cash decimal(7, 2),
   wr_reversed_charge decimal(7, 2),
   wr_account_credit decimal(7, 2),
   wr_net_loss decimal(7, 2)
)`,
        web_sales: `(
   ws_sold_date_sk double,
   ws_sold_time_sk double,
   ws_ship_date_sk double,
   ws_item_sk integer,
   ws_bill_customer_sk double,
   ws_bill_cdemo_sk double,
   ws_bill_hdemo_sk double,
   ws_bill_addr_sk double,
   ws_ship_customer_sk double,
   ws_ship_cdemo_sk double,
   ws_ship_hdemo_sk double,
   ws_ship_addr_sk double,
   ws_web_page_sk double,
   ws_web_site_sk double,
   ws_ship_mode_sk double,
   ws_warehouse_sk double,
   ws_promo_sk double,
   ws_order_number integer,
   ws_quantity double,
   ws_wholesale_cost decimal(5, 2),
   ws_list_price decimal(5, 2),
   ws_sales_price decimal(5, 2),
   ws_ext_discount_amt decimal(7, 2),
   ws_ext_sales_price decimal(7, 2),
   ws_ext_wholesale_cost decimal(7, 2),
   ws_ext_list_price decimal(7, 2),
   ws_ext_tax decimal(6, 2),
   ws_coupon_amt decimal(7, 2),
   ws_ext_ship_cost decimal(7, 2),
   ws_net_paid decimal(7, 2),
   ws_net_paid_inc_tax decimal(7, 2),
   ws_net_paid_inc_ship decimal(7, 2),
   ws_net_paid_inc_ship_tax decimal(7, 2),
   ws_net_profit decimal(7, 2)
)`,
        web_site: `(
   web_site_sk integer,
   web_site_id varchar,
   web_rec_start_date date,
   web_rec_end_date date,
   web_name varchar,
   web_open_date_sk double,
   web_close_date_sk double,
   web_class varchar,
   web_manager varchar,
   web_mkt_id integer,
   web_mkt_class varchar,
   web_mkt_desc varchar,
   web_market_manager varchar,
   web_company_id integer,
   web_company_name varchar,
   web_street_number varchar,
   web_street_name varchar,
   web_street_type varchar,
   web_suite_number varchar,
   web_city varchar,
   web_county varchar,
   web_state varchar,
   web_zip varchar,
   web_country varchar,
   web_gmt_offset decimal(3, 2),
   web_tax_percentage decimal(2, 2)
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
