# GHCNd_hadoop_hive

# Analysis of GHCNd Data Using Hadoop and Hive

## Project Overview
This project analyzes the Global Historical Climatology Network daily (GHCNd) dataset focusing on North America over the last 25 years using Hadoop and Hive for large-scale data processing. The aim is to gain insights into temperature, precipitation, and extreme weather events.

The data is taken from the official website by year : https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year/


## System Requirements:
- **Hadoop 3.x**
- **Hive 3.x**
- **Python 3.x**
- **VirtualBox** (Setup with one master node and 3 data nodes)

## Setup
The general setup for this project would be to create a cluster of with atleast 3 nodes.
- **Virtual Environment**: Utilizing Virtual Box with one master node and three data nodes.
- **Technologies**: Hadoop cluster with Hive installed for advanced querying capabilities.

## Overview

Each file here corresponds to a specific map reduce job. It can be run either on the cluster that we have setup or you can test locally for a small amount of data.

### Original Data Format
The GHCNd dataset consists of daily climate summaries which include various measurements such as temperature (min, max, average) and precipitation. Each record is structured as follows:
- station_id, date, metric, value, m_flag, q_flag, s_flag, obs_time

### Transformed Data Format
After cleaning and processing, the data is transformed to a more analysis-friendly format, which includes only the relevant metrics:

- station_id, country, date, tmin, tmax, tavg, precipitation



## Data Preparation
The GHCNd dataset is prepared through filtering, augmenting, and cleaning operations:
- MapReduce jobs are used to include only relevant temperature and precipitation data.
- Data imputation is performed for missing temperature and precipitation values.
- Data stations with incomplete records for extended periods (in this case 10 days) are excluded.

## Running the Jobs for Data preparation 
Once Hadoop and hive are configured you can run these commands. First run the mapper1 and reducer1 to filter the data based on the countries to be included and data imputation for both temperature and precipitation. Once we get the data in the desired format, we now have to remove stations with missing temperature or precipitation data for more than 10 days. To achieve this, we first find such stations where data is missing for more than 10 days and then filter them from the output that we achieve by running the initial map reduce jobs.


``` bash
hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -mapper mapper11.py -reducer reducer11.py -input /input/2024.csv -output /output11
```
If you are running locally on linux or mac,

Use these commands:

```bash
cat 2024.csv  | ./mapper11.py | sort | ./reducer11.py > output_1

python3 find_invalid.py output_1 > invalid_stations 


python3 filter_invalid.py --excluded-stations invalid_stations > filtered_stations

cat 2024.csv | ./filter_invalid.py invalid_stations> filtered_stations

```


## Project Components

### Temperature Analysis
- **Filtering and Aggregation**: Temperature records are processed based on location and date range, with aggregation options for daily, monthly, or yearly analysis.
- **Comparison**: A MapReduce job compares average annual temperatures between specified regions, facilitating climate analysis over time.

### Precipitation Analysis
- **Average Calculation**: Precipitation data is analyzed to calculate average values over specified periods and locations.
- **Condition Classification**: Classification of days as experiencing "Heavy Rainfall," "Drought," or "Normal" conditions based on defined thresholds.

### Extreme Weather Event Analysis Using Hive
- **Hive Queries**: Developed to identify and analyze heatwaves and cold spells by geographic locations.
- **Frequency and Intensity Analysis**: Queries count occurrences of extreme temperature days and calculate average and peak temperatures during such events.

### Trend Analysis
- **Temperature and Precipitation Trends**: Analyze long-term temperature and precipitation trends by aggregating data monthly and identifying regional variations.



