#!/usr/bin/env python3
"""
Data Engineering Zoomcamp - Module 6 Homework
Quick analysis using Pandas and Spark
"""

import pandas as pd
import pyarrow.parquet as pq
import subprocess
import sys

print("\n" + "="*80)
print("DATA ENGINEERING ZOOMCAMP - MODULE 6 HOMEWORK SOLUTIONS")
print("="*80)

answers = {}

# Q1: Spark Version  
print("\n[Q1] Install Spark and PySpark")
print("-"*80)
try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[*]").appName("HW6").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    spark_version = spark.version
    print(f"✓ Spark Version: {spark_version}")
    answers["Q1"] = spark_version
    spark.stop()
except Exception as e:
    print(f"✗ Error: {e}")

# Q2: Parquet file sizes
print("\n[Q2] Yellow November 2025 - Parquet File Sizes")
print("-"*80)
try:
    # Read parquet file to check its size
    parquet_file = pq.read_table("yellow_tripdata_2025-11.parquet")
    print(f"✓ Total rows: {parquet_file.num_rows:,}")
    
    # Repartition using Spark
    spark = SparkSession.builder.master("local[*]").appName("HW6-Q2").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    yellow_df = spark.read.parquet("yellow_tripdata_2025-11.parquet")
    print(f"✓ Repartitioning to 4 partitions...")
    yellow_df.repartition(4).write.mode("overwrite").parquet("yellow_output")
    
    # Get file sizes
    result = subprocess.run(
        "find yellow_output -name 'part-*.parquet' -exec du -h {} \\; | awk '{print $1}'",
        shell=True, capture_output=True, text=True
    )
    
    sizes = []
    for line in result.stdout.strip().split('\n'):
        if line:
            size_str = line.strip()
            # Parse size (e.g., "17M" -> 17)
            if 'M' in size_str:
                size_mb = float(size_str.replace('M', ''))
                sizes.append(size_mb)
            elif 'K' in size_str:
                size_mb = float(size_str.replace('K', '')) / 1024
                sizes.append(size_mb)
    
    if sizes:
        avg_size = sum(sizes) / len(sizes)
        print(f"  Files created: {sizes}")
        print(f"✓ Average Parquet file size: {avg_size:.1f} MB")
        
        # Find closest answer
        closest = min([6, 25, 75, 100], key=lambda x: abs(x - avg_size))
        print(f"✓ Closest answer: {closest}MB")
        answers["Q2"] = closest
    
    spark.stop()
except Exception as e:
    print(f"✗ Error Q2: {e}")
    import traceback
    traceback.print_exc()

# Q3: Trips on Nov 15
print("\n[Q3] Yellow Taxi Trips on November 15th")
print("-"*80)
try:
    spark = SparkSession.builder.master("local[*]").appName("HW6-Q3").config("spark.sql.shuffle.partitions", "1").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    yellow_df = spark.read.parquet("yellow_tripdata_2025-11.parquet")
    
    from pyspark.sql.functions import dayofmonth
    trips_nov15 = yellow_df.filter(dayofmonth(yellow_df.tpep_pickup_datetime) == 15).count()
    
    print(f"✓ Trips on Nov 15: {trips_nov15:,}")
    
    closest = min([62610, 102340, 162604, 225768], key=lambda x: abs(x - trips_nov15))
    print(f"✓ Closest answer: {closest}")
    answers["Q3"] = closest
    
    spark.stop()
except Exception as e:
    print(f"✗ Error Q3: {e}")

# Q4: Longest trip
print("\n[Q4] Longest Trip Duration")
print("-"*80)
try:
    spark = SparkSession.builder.master("local[*]").appName("HW6-Q4").config("spark.sql.shuffle.partitions", "1").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    yellow_df = spark.read.parquet("yellow_tripdata_2025-11.parquet")
    
    from pyspark.sql.functions import max, unix_timestamp
    trip_duration = ((unix_timestamp(yellow_df.tpep_dropoff_datetime) - 
                      unix_timestamp(yellow_df.tpep_pickup_datetime)) / 3600)
    
    longest = yellow_df.select(trip_duration.alias("hours")).agg(max("hours")).collect()[0][0]
    
    print(f"✓ Longest trip: {longest:.1f} hours")
    
    closest = min([22.7, 58.2, 90.6, 134.5], key=lambda x: abs(x - longest))
    print(f"✓ Closest answer: {closest}")
    answers["Q4"] = closest
    
    spark.stop()
except Exception as e:
    print(f"✗ Error Q4: {e}")

# Q5: Spark UI Port
print("\n[Q5] Spark UI Port")
print("-"*80)
print(f"✓ Spark UI runs on port: 4040")
answers["Q5"] = "4040"

# Q6: Least frequent zone
print("\n[Q6] Least Frequent Pickup Location Zone")
print("-"*80)
try:
    spark = SparkSession.builder.master("local[*]").appName("HW6-Q6").config("spark.sql.shuffle.partitions", "4").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    zones_df = spark.read.csv("taxi_zone_lookup.csv", header=True, inferSchema=True)
    yellow_df = spark.read.parquet("yellow_tripdata_2025-11.parquet")
    
    zones_df.createOrReplaceTempView("zones")
    yellow_df.createOrReplaceTempView("yellow_trips")
    
    result = spark.sql("""
        SELECT z.Zone, COUNT(*) as trips
        FROM yellow_trips y
        JOIN zones z ON y.PULocationID = z.LocationID
        GROUP BY z.Zone
        ORDER BY trips ASC
        LIMIT 1
    """).collect()
    
    if result:
        zone_name = result[0]['Zone']
        trips_count = result[0]['trips']
        print(f"✓ Least frequent zone: {zone_name} ({trips_count:,} trips)")
        answers["Q6"] = zone_name
    
    spark.stop()
except Exception as e:
    print(f"✗ Error Q6: {e}")
    import traceback
    traceback.print_exc()

# Final Summary
print("\n" + "="*80)
print("FINAL ANSWERS")
print("="*80)
print(f"Q1 - Spark Version: {answers.get('Q1', 'N/A')}")
print(f"Q2 - Average Parquet Size: {answers.get('Q2', 'N/A')} MB")

q3_val = answers.get('Q3', 'N/A')
if isinstance(q3_val, int):
    print(f"Q3 - Trips on Nov 15: {q3_val:,}")
else:
    print(f"Q3 - Trips on Nov 15: {q3_val}")

print(f"Q4 - Longest trip: {answers.get('Q4', 'N/A')} hours")
print(f"Q5 - Spark UI Port: {answers.get('Q5', 'N/A')}")
print(f"Q6 - Least frequent zone: {answers.get('Q6', 'N/A')}")
print("="*80 + "\n")
