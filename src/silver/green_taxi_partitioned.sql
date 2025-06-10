SELECT
  md5(
    concat_ws(
      '|',
      CAST(VendorID AS STRING),
      CAST(lpep_pickup_datetime AS STRING),
      CAST(lpep_dropoff_datetime AS STRING),
      CAST(PULocationID AS STRING),
      CAST(DOLocationID AS STRING)
    )
  ) AS trip_id,
  VendorID as vendor_id,
  -- Tradução do código VendorID
  CASE
    VendorID
    WHEN 1 THEN 'Creative Mobile Technologies, LLC'
    WHEN 2 THEN 'Curb Mobility, LLC'
    WHEN 6 THEN 'Myle Technologies Inc'
    ELSE 'Unknown'
  END AS vendor_name,
lpep_pickup_datetime as pickup_datetime,
  lpep_dropoff_datetime as dropoff_datetime,
  -- Duração da viagem (em minutos)
  ROUND(
    (UNIX_TIMESTAMP(lpep_dropoff_datetime) - UNIX_TIMESTAMP(lpep_pickup_datetime)) / 60, 2
  ) AS trip_duration_minutes,
  passenger_count,
  ROUND(trip_distance * 1.60934, 2) as trip_distance_km,
  -- Tradução do código RatecodeID
  CASE
    RatecodeID
    WHEN 1 THEN 'Standard rate'
    WHEN 2 THEN 'JFK'
    WHEN 3 THEN 'Newark'
    WHEN 4 THEN 'Nassau or Westchester'
    WHEN 5 THEN 'Negotiated fare'
    WHEN 6 THEN 'Group ride'
    WHEN 99 THEN 'Null/unknown'
    ELSE 'Other'
  END AS ratecode_description,
  RatecodeID as ratecode_id,
  -- Flag store_and_fwd
  CASE
    store_and_fwd_flag
    WHEN 'Y' THEN 'store and forward trip'
    WHEN 'N' THEN 'not a store and forward trip'
    ELSE 'unknown'
  END AS store_and_fwd_description,
  store_and_fwd_flag,
  PULocationID as pickup_location_id,
  DOLocationID as dropoff_location_id,
  -- Tradução do payment_type
  CASE
    payment_type
    WHEN 0 THEN 'Flex Fare trip'
    WHEN 1 THEN 'Credit card'
    WHEN 2 THEN 'Cash'
    WHEN 3 THEN 'No charge'
    WHEN 4 THEN 'Dispute'
    WHEN 5 THEN 'Unknown'
    WHEN 6 THEN 'Voided trip'
    ELSE 'Other'
  END AS payment_type_description,
  payment_type AS payment_type_id,
  -- Valores financeiros
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  congestion_surcharge,
  ehail_fee,
  total_amount,
  pickup_year_month
FROM
  bronze.taxi.green_taxi_partitioned
WHERE
  lpep_pickup_datetime IS NOT NULL
  AND lpep_dropoff_datetime IS NOT NULL
  AND trip_distance > 0
  AND passenger_count >= 0
  AND lpep_pickup_datetime <= GETDATE()
  AND lpep_dropoff_datetime <= GETDATE()