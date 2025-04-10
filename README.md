# 📡 IoT Sensor Data Analysis with Spark SQL

This project analyzes simulated IoT sensor data using **PySpark SQL**, applying filtering, aggregation, time-series breakdown, window functions, and pivoting to gain insights on sensor behavior across buildings and time.

---

## 📁 Dataset Overview

**Input File:**
- `sensor_data.csv`

**Schema:**
| Column       | Type      | Description                          |
|--------------|-----------|--------------------------------------|
| `sensor_id`  | Integer   | Unique ID of the sensor              |
| `timestamp`  | Timestamp | Time of the sensor reading           |
| `temperature`| Float     | Temperature recorded (°C)            |
| `humidity`   | Float     | Humidity recorded (%)                |
| `location`   | String    | Sensor location (building/floor)     |
| `sensor_type`| String    | Sensor classification (TypeA/B/C)    |

---

## ✅ Task 1: Load & Basic Exploration

- Load `sensor_data.csv` using Spark
- Create a temporary SQL view `sensor_readings`
- Display:
  - First 5 rows
  - Total record count
  - Distinct locations
- Save the **first 5 records** to `task1_output.csv`

### 🖥 Sample Output:

+---------+-------------------+-----------+--------+----------------+-----------+
|sensor_id|          timestamp|temperature|humidity|        location|sensor_type|
+---------+-------------------+-----------+--------+----------------+-----------+
|     1005|2025-04-06 19:32:25|      16.25|   54.59|BuildingA_Floor2|      TypeB|
|     1045|2025-04-07 21:26:48|      29.96|   52.11|BuildingB_Floor1|      TypeB|
|     1082|2025-04-08 15:56:20|      20.31|    39.5|BuildingB_Floor1|      TypeC|
|     1073|2025-04-09 03:29:52|      23.16|   49.68|BuildingA_Floor1|      TypeC|
|     1009|2025-04-07 02:57:39|      22.12|   35.42|BuildingB_Floor1|      TypeA|
+---------+-------------------+-----------+--------+----------------+-----------+
only showing top 5 rows

### 📊 Analysis:
- Data includes multiple floors and buildings with varied sensor types.
- First few rows help validate schema and data quality.

---

## ✅ Task 2: Filtering & Aggregations

- Filter temperature readings:
  - Count in-range (18–30°C) vs. out-of-range
- Compute average temperature and humidity **by location**

### Output (`task2_output.csv`):

location,avg_temperature,avg_humidity
BuildingA_Floor2,25.26514285714286,55.95771428571429
BuildingB_Floor1,25.145086956521734,53.48113043478262
BuildingA_Floor1,25.125972850678735,55.121764705882384
BuildingB_Floor2,24.618289962825287,56.152193308550196


### 📊 Analysis:
- BuildingB_Floor1 is the warmest and driest.
- Slight humidity variation across floors.

---

## ✅ Task 3: Time-Based Analysis

- Extract `hour_of_day` from timestamps
- Group and average temperatures by hour
- Save to `task3_output.csv`

### Sample Output:

hour_of_day,avg_temp
0,25.156216216216205
1,25.533749999999998
2,23.892000000000003
3,26.251162790697677
4,23.76133333333333
5,25.24092592592592
6,25.453333333333333
7,24.551999999999996
8,23.2725
9,23.995428571428572
10,24.014687500000004
11,22.948124999999994
12,25.593750000000004
13,25.170476190476194
14,26.21744186046512
15,24.75133333333333
16,24.535675675675673
17,26.604363636363637
18,24.649722222222223
19,24.528
20,26.140857142857147
21,25.40333333333333
22,25.99022727272727
23,25.71127659574468

```

### 📊 Analysis:
- Hours 8 AM to 10 AM and 4–6 PM show slightly higher temperatures.
- Most readings are in the 23–26 °C range, suggesting controlled environments.

---

## ✅ Task 4: Sensor Ranking (Window Function)

- Calculate each sensor’s average temperature
- Apply `dense_rank()` on descending order
- Save top 5 to `task4_output.csv`

### Output:

sensor_id,avg_temp,rank_temp
1000,30.611,1
1003,28.354000000000003,2
1065,28.235714285714284,3
1059,28.093333333333337,4
1045,27.867,5

### 📊 Analysis:
- Sensor 1002 is the hottest on average — may need recalibration or is located in a hotter environment.

---

## ✅ Task 5: Pivot by Hour and Location

- Pivot table with:
  - `location` as rows
  - `hour_of_day` (0–23) as columns
  - `avg(temperature)` in each cell
- Save to `task5_output.csv`

### Sample Output (partial):

location,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
BuildingA_Floor1,25.55,25.1,24.58,24.06,23.34,25.67,30.79,23.57,23.62,26.39,23.9,20.3,25.52,25.69,27.91,22.91,25.27,26.8,27.83,25.72,29.18,23.39,22.74,27.53
BuildingB_Floor2,21.06,25.2,24.2,26.12,23.5,23.37,24.14,27.98,22.19,23.75,21.72,22.16,27.88,24.21,28.43,23.56,23.62,22.9,23.63,23.59,27.0,26.28,25.18,27.61
BuildingA_Floor2,27.1,25.91,24.6,27.8,22.99,24.7,26.15,25.29,23.96,21.56,22.91,25.59,24.75,25.15,23.2,26.04,25.39,27.35,24.46,24.84,23.65,25.06,27.75,24.77
BuildingB_Floor1,25.92,25.74,22.75,25.75,25.21,26.79,25.26,20.94,23.35,24.42,28.16,23.57,24.39,25.99,24.07,28.93,21.8,29.38,22.53,23.33,26.86,25.54,27.04,23.73


### 📊 Analysis:
- BuildingA_Floor2 shows highest temps around hours 2–4 and 14–16.
- Useful for HVAC scheduling or identifying overused zones.

---

## ⚙️ How to Run

In **GitHub Codespaces** terminal:

```bash
spark-submit task1_basic_exploration.py
spark-submit task2_filter_aggregate.py
spark-submit task3_time_analysis.py
spark-submit task4_window_function.py
spark-submit task5_pivot_interpretation.py
```

---

## 📁 Output Folder Structure

```

├── task1_output.csv
├── task2_output.csv
├── task3_output.csv
├── task4_output.csv
├── task5_output.csv
```

---

## 🛠️ Problems Faced & Fixes

### ❌ Rounding & Formatting (Task 5)
- **Issue**: Pivot values had too many decimals.
- **Fix**: Used `round(column, 2)` dynamically for all columns post-pivot.

### ❌ Hour-based Sorting in Pivot
- **Issue**: Output CSV did not preserve hour order (0–23).
- **Fix**: Spark handles pivot in correct order if hour column is numeric, ensured hours were casted as `int`.

### ❌ Sensor Ranking Confusion
- Spark window functions required explicit partitioning/ordering. Fixed using `DenseRank().over(Window.orderBy(...))`


