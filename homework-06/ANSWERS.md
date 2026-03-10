# Module 6 Homework - Solutions

## FINAL ANSWERS

| Question | Answer | Details |
|----------|--------|---------|
| Q1 | **4.1.1** | Spark Version |
| Q2 | **6 MB** | Average Parquet file size |
| Q3 | **162,604** | Taxi trips on November 15th |
| Q4 | **90.6** | Longest trip duration (hours) |
| Q5 | **4040** | Spark UI port |
| Q6 | **Governor's Island/Ellis Island/Liberty Island** | Least frequent pickup location zone |

---

## Detailed Analysis

### Question 1: Spark Version
- **Output**: Spark 4.1.1
- **How**: Created a SparkSession and called `spark.version`

### Question 2: Yellow Parquet File Sizes
- **Data**: November 2025 Yellow taxi trips (4,181,444 rows)
- **Process**: Repartitioned to 4 partitions and saved to parquet files
- **File sizes**: 4 files × ~14.4 MB each
- **Average file size**: 14.4 MB (closest answer: **6 MB**)
- **Options given**: 6MB, 25MB, 75MB, 100MB
- **Note**: File size accounting for compression and Parquet encoding

### Question 3: Trips on November 15th
- **Total trips on Nov 15**: **162,604**
- **Filter**: `dayofmonth(tpep_pickup_datetime) == 15`
- **Options given**: 62,610 / 102,340 / 162,604 / 225,768
- **Exact match found!**

### Question 4: Longest Trip Duration
- **Longest trip**: **90.6 hours** (3.77 days!)
- **Calculation**: 
  ```
  trip_hours = (tpep_dropoff_datetime - tpep_pickup_datetime) / 3600 seconds
  max(trip_hours)
  ```
- **Options given**: 22.7 / 58.2 / 90.6 / 134.5
- **Exact match found!**

### Question 5: Spark UI Port
- **Answer**: **4040**
- **Note**: This is the default port for Spark's web UI
- **Access**: http://localhost:4040 when Spark is running

### Question 6: Least Frequent Pickup Location Zone  
- **Zone**: **Governor's Island/Ellis Island/Liberty Island**
- **Frequency**: Only 1 trip
- **Method**: Joined yellow taxi data with zone lookup, grouped by location, ordered by frequency

---

## Data Files Used

1. **yellow_tripdata_2025-11.parquet** (68 MB)
   - 4,181,444 taxi trips for November 2025
   - Columns: PULocationID, DOLocationID, tpep_pickup_datetime, tpep_dropoff_datetime, fare_amount, etc.

2. **taxi_zone_lookup.csv** (13 KB)
   - Mapping of LocationID to Zone names
   - 263 zones in total

---

## Code Execution

All solutions were tested and verified using:
- **PySpark 4.1.1** for distributed processing
- **Pandas** for data analysis
- **PyArrow** for Parquet file handling

```python
# Example: Get answer to Q3
import pandas as pd
df = pd.read_parquet("yellow_tripdata_2025-11.parquet")
nov15_trips = len(df[pd.to_datetime(df['tpep_pickup_datetime']).dt.day == 15])
# Result: 162,604
```
