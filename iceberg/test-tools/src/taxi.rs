//! A synthetic table shaped like the NYC TLC Yellow Taxi trip records.
//!
//! The values are made up but reproducible. They're drawn from `rand_pcg`'s
//! `Pcg64Mcg`, seeded with the day, whose output `rand` documents as portable
//! across platforms. The formulas only add, multiply, divide and round, which
//! IEEE 754 specifies exactly, so they don't depend on the platform's math
//! library either.

use std::sync::Arc;

use datafusion::arrow::array::{
    Date32Array, Float64Array, Int32Array, Int64Array, RecordBatch, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use datafusion::error::Result;
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
use iceberg::TableIdent;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Transform, Type};
use rand::distr::Distribution;
use rand::distr::weighted::WeightedIndex;
use rand::{RngExt, SeedableRng};
use rand_pcg::Pcg64Mcg;

use crate::{FixtureCatalog, FixtureTable};

/// The number of days of trips, each of which is one partition and one file.
pub const TAXI_DAYS: usize = 7;
/// The number of trips picked up on each day.
pub const TAXI_TRIPS_PER_DAY: usize = 25_000;
/// The first pickup date, 2024-01-08, in days since the Unix epoch.
pub const TAXI_FIRST_DAY: i32 = 19_730;

const MICROS_PER_SECOND: i64 = 1_000_000;
const SECONDS_PER_DAY: i64 = 86_400;

/// Creates table `ident` of trips, partitioned by `pickup_date`, with one
/// snapshot that adds one file per day, in date order.
///
/// Each file is a single row group with column chunk statistics but without
/// page indexes.
pub async fn create_taxi_table(
    fixtures: &FixtureCatalog,
    ident: TableIdent,
) -> Result<FixtureTable> {
    let mut table = fixtures
        .table(ident, taxi_schema())
        .partition_by("pickup_date", Transform::Identity)
        .create()
        .await?;
    let mut append = table.append().writer_properties(
        WriterProperties::builder()
            .set_max_row_group_row_count(Some(TAXI_TRIPS_PER_DAY))
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_offset_index_disabled(true)
            .build(),
    );
    for day in 0..TAXI_DAYS {
        append = append.file(taxi_trips(day)?);
    }
    append.commit().await?;
    Ok(table)
}

/// The taxi table's schema: 13 optional columns with field ids 1 to 13, in
/// order.
pub fn taxi_schema() -> Schema {
    let columns = [
        ("vendor_id", PrimitiveType::Int),
        ("pickup_at", PrimitiveType::Timestamp),
        ("dropoff_at", PrimitiveType::Timestamp),
        ("passenger_count", PrimitiveType::Long),
        ("trip_distance", PrimitiveType::Double),
        ("pickup_location_id", PrimitiveType::Int),
        ("dropoff_location_id", PrimitiveType::Int),
        ("payment_type", PrimitiveType::Long),
        ("fare_amount", PrimitiveType::Double),
        ("tip_amount", PrimitiveType::Double),
        ("tolls_amount", PrimitiveType::Double),
        ("total_amount", PrimitiveType::Double),
        ("pickup_date", PrimitiveType::Date),
    ];
    Schema::builder()
        .with_fields(
            columns.into_iter().zip(1..).map(|((name, ty), id)| {
                NestedField::optional(id, name, Type::Primitive(ty)).into()
            }),
        )
        .build()
        .expect("the taxi schema is valid")
}

/// How often each passenger count occurs, in percent. `None` is a missing
/// count.
const PASSENGER_COUNTS: [(Option<i64>, u32); 7] = [
    (None, 5),
    (Some(1), 70),
    (Some(2), 13),
    (Some(3), 5),
    (Some(4), 3),
    (Some(5), 2),
    (Some(0), 2),
];

/// How often each payment type occurs among trips with a passenger count, in
/// percent. Trips without one have payment type 0, as in the real records.
const PAYMENT_TYPES: [(i64, u32); 4] = [(1, 78), (2, 18), (3, 3), (4, 1)];

/// The trips picked up on the `day`th day, ordered by pickup time.
///
/// Pickup times are distinct within a day, so ordering by them is total.
/// About 5% of trips have no passenger count; those have payment type 0, as
/// in the real records.
pub fn taxi_trips(day: usize) -> Result<RecordBatch> {
    let mut rng = Pcg64Mcg::seed_from_u64(day as u64);
    let passenger_counts = weighted(&PASSENGER_COUNTS);
    let payment_types = weighted(&PAYMENT_TYPES);
    let pickup_date = TAXI_FIRST_DAY + day as i32;
    let mut pickup_at = i64::from(pickup_date) * SECONDS_PER_DAY;
    let mut trips = Trips::default();
    for _ in 0..TAXI_TRIPS_PER_DAY {
        pickup_at += rng.random_range(1..=5);
        let vendor_id = if rng.random_bool(0.25) { 1 } else { 2 };
        let passenger_count = PASSENGER_COUNTS[passenger_counts.sample(&mut rng)].0;
        // Mostly short trips, with a few long ones, such as to the airports.
        let short: f64 = rng.random();
        let mut trip_distance = 0.4 + 9.6 * short * short * short;
        if rng.random_bool(0.04) {
            trip_distance += rng.random_range(8.0..30.0);
        }
        let trip_distance = cents(trip_distance);
        let duration = (trip_distance * 150.0) as i64 + rng.random_range(90..=900);
        let payment_type = match passenger_count {
            None => 0,
            Some(_) => PAYMENT_TYPES[payment_types.sample(&mut rng)].0,
        };
        let fare_amount = cents(3.0 + 2.5 * trip_distance + duration as f64 / 150.0);
        let tip_amount = if payment_type == 1 {
            cents(fare_amount * rng.random_range(0.1..0.25))
        } else {
            0.0
        };
        let tolls_amount = if trip_distance > 9.0 && rng.random_bool(0.35) {
            6.94
        } else {
            0.0
        };

        trips.vendor_id.push(vendor_id);
        trips.pickup_at.push(pickup_at * MICROS_PER_SECOND);
        trips
            .dropoff_at
            .push((pickup_at + duration) * MICROS_PER_SECOND);
        trips.passenger_count.push(passenger_count);
        trips.trip_distance.push(trip_distance);
        trips.pickup_location_id.push(rng.random_range(1..=265));
        trips.dropoff_location_id.push(rng.random_range(1..=265));
        trips.payment_type.push(payment_type);
        trips.fare_amount.push(fare_amount);
        trips.tip_amount.push(tip_amount);
        trips.tolls_amount.push(tolls_amount);
        trips
            .total_amount
            .push(cents(fare_amount + tip_amount + tolls_amount + 3.5));
    }
    trips.into_batch(pickup_date)
}

/// The columns of the trips being generated, in the taxi schema's order.
#[derive(Default)]
struct Trips {
    vendor_id: Vec<i32>,
    pickup_at: Vec<i64>,
    dropoff_at: Vec<i64>,
    passenger_count: Vec<Option<i64>>,
    trip_distance: Vec<f64>,
    pickup_location_id: Vec<i32>,
    dropoff_location_id: Vec<i32>,
    payment_type: Vec<i64>,
    fare_amount: Vec<f64>,
    tip_amount: Vec<f64>,
    tolls_amount: Vec<f64>,
    total_amount: Vec<f64>,
}

impl Trips {
    fn into_batch(self, pickup_date: i32) -> Result<RecordBatch> {
        let timestamp = DataType::Timestamp(TimeUnit::Microsecond, None);
        let schema = ArrowSchema::new(vec![
            Field::new("vendor_id", DataType::Int32, true),
            Field::new("pickup_at", timestamp.clone(), true),
            Field::new("dropoff_at", timestamp, true),
            Field::new("passenger_count", DataType::Int64, true),
            Field::new("trip_distance", DataType::Float64, true),
            Field::new("pickup_location_id", DataType::Int32, true),
            Field::new("dropoff_location_id", DataType::Int32, true),
            Field::new("payment_type", DataType::Int64, true),
            Field::new("fare_amount", DataType::Float64, true),
            Field::new("tip_amount", DataType::Float64, true),
            Field::new("tolls_amount", DataType::Float64, true),
            Field::new("total_amount", DataType::Float64, true),
            Field::new("pickup_date", DataType::Date32, true),
        ]);
        let rows = self.vendor_id.len();
        Ok(RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(self.vendor_id)),
                Arc::new(TimestampMicrosecondArray::from(self.pickup_at)),
                Arc::new(TimestampMicrosecondArray::from(self.dropoff_at)),
                Arc::new(Int64Array::from(self.passenger_count)),
                Arc::new(Float64Array::from(self.trip_distance)),
                Arc::new(Int32Array::from(self.pickup_location_id)),
                Arc::new(Int32Array::from(self.dropoff_location_id)),
                Arc::new(Int64Array::from(self.payment_type)),
                Arc::new(Float64Array::from(self.fare_amount)),
                Arc::new(Float64Array::from(self.tip_amount)),
                Arc::new(Float64Array::from(self.tolls_amount)),
                Arc::new(Float64Array::from(self.total_amount)),
                Arc::new(Date32Array::from(vec![pickup_date; rows])),
            ],
        )?)
    }
}

/// Rounds `amount` to two decimal places.
fn cents(amount: f64) -> f64 {
    (amount * 100.0).round() / 100.0
}

/// Samples indices into `choices`, in proportion to their weights.
fn weighted<T>(choices: &[(T, u32)]) -> WeightedIndex<u32> {
    WeightedIndex::new(choices.iter().map(|(_, weight)| *weight)).expect("the weights are positive")
}
