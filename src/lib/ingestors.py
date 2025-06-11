import delta
from utils import import_schema, create_merge_condition, import_query, extract_from, format_query_cdf, date_range, table_exists
import tqdm

class Ingestor:

    def __init__(self, spark, catalog, schema_name, table_name, data_format):
        self.spark = spark 
        self.catalog = catalog
        self.schema_name = schema_name
        self.table_name = table_name
        self.format = data_format
        self.set_schema()
        self.checkpoint_location = f"/Volumes/raw/{self.schema_name}/cdc/{self.table_name}_checkpoint/"

    def set_schema(self):
        self.data_schema = import_schema(self.table_name)

    def load(self, path):
        df = (self.spark
                  .read
                  .format(self.format)
                  .schema(self.data_schema)
                  .load(path))
        return df
        
    def save(self, df):
        (df.write
           .format("delta")
           .mode("overwrite")
           .saveAsTable(f"{self.catalog}.{self.schema_name}.{self.table_name}"))
        return True
        
    def execute(self, path):
        df = self.load(path)
        return self.save(df)
    
class IngestorCDC(Ingestor):

    def __init__(self, spark, catalog, schema_name, table_name, data_format, id_field, timestamp_field):
        super().__init__(spark, catalog, schema_name, table_name, data_format)
        self.id_field = id_field
        self.timestamp_field = timestamp_field
        self.set_deltatable()

    def set_deltatable(self):
        table_name = f"{self.catalog}.{self.schema_name}.{self.table_name}"
        self.deltatable = delta.DeltaTable.forName(self.spark, table_name)

    def upsert(self, df):
        df.createOrReplaceGlobalTempView(f"view_{self.table_name}")
        query = f'''
            SELECT *
            FROM global_temp.view_{self.table_name}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY {self.id_field} ORDER BY {self.timestamp_field} DESC) = 1
        '''

        merge_condition = create_merge_condition(id_field = self.id_field, left_alias ='b', right_alias = 'd')

        df_cdc = self.spark.sql(query)

        (self.deltatable
             .alias("b")
             .merge(df_cdc.alias("d"), merge_condition) 
             .whenMatchedDelete(condition = "d.OP = 'D'")
             .whenMatchedUpdateAll(condition = "d.OP = 'U'")
             .whenNotMatchedInsertAll(condition = "d.OP = 'I' OR d.OP = 'U'")
             .execute())

    def load(self, path):
        df = (self.spark
                  .readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", self.format)
                  .schema(self.data_schema)
                  .load(path))
        return df
    
    def save(self, df):
        stream = (df.writeStream
                   .option("checkpointLocation", self.checkpoint_location)
                   .foreachBatch(lambda df, batchID: self.upsert(df))
                   .trigger(availableNow=True))
        return stream.start()
    

class IngestorCDF(IngestorCDC):

    def __init__(self, spark, catalog, schema_name, table_name, id_field, id_field_from):
        
        super().__init__(spark=spark,
                         catalog=catalog,
                         schema_name=schema_name,
                         table_name=table_name,
                         data_format='delta',
                         id_field=id_field,
                         timestamp_field='_commit_timestamp')
        
        self.id_field_from = id_field_from
        self.set_query()
        self.checkpoint_location = f"/Volumes/raw/{schema_name}/cdc/{catalog}_{table_name}_checkpoint/"

    def set_schema(self):
        return

    def set_query(self):
        query = import_query(self.table_name)
        self.from_table = extract_from(query=query)
        self.original_query = query
        self.query = format_query_cdf(query, "{df}")

    def load(self):
        df = (self.spark.readStream
                   .format('delta')
                   .option("readChangeFeed", "true")
                   .table(self.from_table))
        return df
    
    def save(self, df):
        stream = (df.writeStream
                    .option("checkpointLocation", self.checkpoint_location)
                    .foreachBatch(lambda df, batchID: self.upsert(df) )
                    .trigger(availableNow=True))
        return stream.start()
    
    def upsert(self, df):
        df.createOrReplaceGlobalTempView(f"silver_{self.table_name}")

        query_last = f"""
        SELECT *
        FROM global_temp.silver_{self.table_name}
        WHERE _change_type <> 'update_preimage'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY {self.id_field_from} ORDER BY _commit_timestamp DESC) = 1
        """
        df_last = self.spark.sql(query_last)
        df_upsert = self.spark.sql(self.query, df=df_last)

        merge_condition = create_merge_condition(id_field = self.id_field, left_alias ='s', right_alias = 'd')

        (self.deltatable
             .alias("s")
             .merge(df_upsert.alias("d"), merge_condition) 
             .whenMatchedDelete(condition = "d._change_type = 'delete'")
             .whenMatchedUpdateAll(condition = "d._change_type = 'update_postimage'")
             .whenNotMatchedInsertAll(condition = "d._change_type = 'insert' OR d._change_type = 'update_postimage'")
               .execute())

    def execute(self):
        df = self.load()
        return self.save(df)
    


class IngestorCubo:

    def __init__(self, spark, catalog, schema_name, table_name, taxi_type='yellow'):
        self.spark = spark
        self.catalog = catalog
        self.schema_name = schema_name
        self.table_name = table_name
        self.taxi_type = taxi_type
        self.table = f'{catalog}.{schema_name}.{table_name}'

    def load_daily_reports(self, dt_ref):
        """Load daily reports for a specific date and taxi type using updated Silver schema"""
        
        source_table = f'silver.taxi.{self.taxi_type}_taxi'
            
        query = f"""
        WITH base AS (
            SELECT
                DATE(pickup_datetime) AS pickup_date,
                trip_duration_minutes,
                total_amount,
                fare_amount,
                tip_amount,
                tolls_amount,
                improvement_surcharge,
                extra,
                trip_distance_km, -- Already converted to km in Silver
                passenger_count,
                vendor_name,
                pickup_location_id,
                dropoff_location_id,
                payment_type_id
            FROM {source_table}
            WHERE DATE(pickup_datetime) = '{dt_ref}'
        ),
        aggregated AS (
            SELECT
                pickup_date,
                COUNT(*) AS num_trips,
                SUM(passenger_count) AS total_passengers,
                SUM(total_amount) AS total_revenue,
                SUM(fare_amount) AS total_fare,
                SUM(tip_amount) AS total_tips,
                SUM(tolls_amount) AS total_tolls,
                SUM(extra) AS total_extra,
                AVG(total_amount) AS avg_total_amount,
                PERCENTILE(total_amount, 0.5) AS median_total_amount,
                
                SUM(CASE WHEN vendor_name = 'Creative Mobile Technologies, LLC' THEN 1 ELSE 0 END) AS num_trips_cmt,
                SUM(CASE WHEN vendor_name = 'Curb Mobility, LLC' THEN 1 ELSE 0 END) AS num_trips_curb,
                SUM(CASE WHEN vendor_name = 'Myle Technologies Inc' THEN 1 ELSE 0 END) AS num_trips_myle,
                SUM(CASE WHEN vendor_name = 'Helix' THEN 1 ELSE 0 END) AS num_trips_helix,
                SUM(CASE WHEN vendor_name = 'Creative Mobile Technologies, LLC' THEN total_amount ELSE 0 END) AS total_revenue_cmt,
                SUM(CASE WHEN vendor_name = 'Curb Mobility, LLC' THEN total_amount ELSE 0 END) AS total_revenue_curb,
                SUM(CASE WHEN vendor_name = 'Myle Technologies Inc' THEN total_amount ELSE 0 END) AS total_revenue_myle,
                SUM(CASE WHEN vendor_name = 'Helix' THEN total_amount ELSE 0 END) AS total_revenue_helix,
                
                COUNT(DISTINCT pickup_location_id) AS distinct_pickup_locations,
                COUNT(DISTINCT dropoff_location_id) AS distinct_dropoff_locations,
                
                AVG(trip_distance_km) AS avg_trip_distance_km,
                AVG(trip_duration_minutes) AS avg_trip_duration_min,
                SUM(trip_distance_km) / NULLIF(SUM(trip_duration_minutes), 0) * 60 AS avg_speed_kmph,
                SUM(total_amount) / NULLIF(SUM(trip_duration_minutes), 0) AS revenue_per_minute,
                SUM(total_amount) / NULLIF(SUM(trip_distance_km), 0) AS revenue_per_km,
                
                AVG(CASE WHEN total_amount > 0 THEN tip_amount / total_amount ELSE NULL END) AS avg_tip_pct,
                COUNT(CASE WHEN tip_amount > 0 THEN 1 END) * 1.0 / COUNT(*) AS pct_trips_with_tip,
                
                COUNT(CASE WHEN trip_distance_km <= 1.6 THEN 1 END) AS short_trips, -- <= 1 mile in km
                COUNT(CASE WHEN trip_distance_km >= 16.1 THEN 1 END) AS long_trips, -- >= 10 miles in km
                
                COUNT(DISTINCT payment_type_id) AS payment_methods_used,
                COUNT(CASE WHEN payment_type_id = 1 THEN 1 END) AS num_credit_card,
                COUNT(CASE WHEN payment_type_id = 2 THEN 1 END) AS num_cash,
                COUNT(CASE WHEN payment_type_id = 3 THEN 1 END) AS num_no_charge,
                COUNT(CASE WHEN payment_type_id = 4 THEN 1 END) AS num_dispute,
                COUNT(CASE WHEN payment_type_id = 5 THEN 1 END) AS num_unknown,
                COUNT(CASE WHEN payment_type_id = 6 THEN 1 END) AS num_voided,
                
                COUNT(CASE WHEN passenger_count = 0 THEN 1 END) AS zero_passenger_trips,
                COUNT(CASE WHEN total_amount <= 0 THEN 1 END) AS invalid_total_trips
            FROM base
            GROUP BY pickup_date
        )
        SELECT 
            pickup_date,
            num_trips, total_passengers, total_revenue, total_fare, total_tips,
            total_tolls, total_extra, avg_total_amount, median_total_amount,
            num_trips_cmt, num_trips_curb, num_trips_myle, num_trips_helix,
            total_revenue_cmt, total_revenue_curb, total_revenue_myle, total_revenue_helix,
            distinct_pickup_locations, distinct_dropoff_locations,
            avg_trip_distance_km, avg_trip_duration_min, avg_speed_kmph,
            revenue_per_minute, revenue_per_km,
            avg_tip_pct, pct_trips_with_tip,
            short_trips, long_trips,
            payment_methods_used, num_credit_card, num_cash, num_no_charge,
            num_dispute, num_unknown, num_voided,
            zero_passenger_trips, invalid_total_trips,
            '{self.taxi_type}' as taxi_type,
            current_timestamp() as created_at
        FROM aggregated
        """
        return self.spark.sql(query)

    def save(self, df, dt_ref):
        # Delete existing data for the date and taxi type
        self.spark.sql(f"DELETE FROM {self.table} WHERE pickup_date = '{dt_ref}' AND taxi_type = '{self.taxi_type}'")
        
        # Insert new data
        df.write.format("delta").mode("append").saveAsTable(self.table)

    def backfill(self, dt_start, dt_stop):
        dates = date_range(dt_start, dt_stop)

        if not table_exists(self.spark, self.catalog, self.schema_name, self.table_name):
            df = self.load_daily_reports(dt_ref = dates.pop(0))
            df.write.format("delta").mode("overwrite").partitionBy("taxi_type").saveAsTable(self.table)

        for dt in tqdm.tqdm(dates):
            df = self.load_daily_reports(dt_ref=dt)
            self.save(df=df, dt_ref=dt)