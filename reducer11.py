#!/usr/bin/env python3
import sys
from collections import defaultdict, deque


def get_country_by_station(station_id):
    if station_id.startswith("US"):
        return "USA"
    elif station_id.startswith("CA"):
        return "CANADA"
    elif station_id.startswith("MX"):
        return "MEXICO"
    return None


def impute_temperatures(temp_data):
    tmin = temp_data.get('TMIN', None)
    tmax = temp_data.get('TMAX', None)
    tavg = temp_data.get('TAVG', None)

    try:
        if tmin is not None and tmax is not None:
            tmin, tmax = float(tmin), float(tmax)
            if tavg is None:
                temp_data['TAVG'] = str((tmin + tmax) / 2)
        if tmin is not None and tavg is not None:
            tmin, tavg = float(tmin), float(tavg)
            if tmax is None:
                temp_data['TMAX'] = str(2 * tavg - tmin)
        if tmax is not None and tavg is not None:
            tmax, tavg = float(tmax), float(tavg)
            if tmin is None:
                temp_data['TMIN'] = str(2 * tavg - tmax)
    except ValueError:
        pass  # Handle conversion error if necessary

    return temp_data


def impute_precipitation(prcp_history):
    if prcp_history:
        avg_prcp = sum(prcp_history) / len(prcp_history)
        return str(avg_prcp)
    return '0'


current_station_id = None
temp_data = {}
prcp_history = deque(maxlen=3)

for line in sys.stdin:
    line = line.strip()
    parts = line.split('\t')
    if len(parts) != 2:
        continue
    station_date, metric_value = parts
    metric, value = metric_value.split(',', 1)

    station_id, date = station_date.split(',', 1)

    if station_id != current_station_id:
        # Output the accumulated data for the previous station
        if current_station_id:
            for date, metrics in temp_data.items():
                metrics = impute_temperatures(metrics)
                prcp = metrics.get('PRCP', impute_precipitation(prcp_history))
                print(f"{current_station_id},{get_country_by_station(current_station_id)},{date},{metrics.get('TMIN', '')},{metrics.get('TMAX', '')},{metrics.get('TAVG', '')},{prcp}")
            temp_data.clear()
            prcp_history.clear()

        current_station_id = station_id

    if metric == 'PRCP':
        if value:  # If precipitation value is present, use it and update the history
            prcp_history.append(int(float(value)))
        temp_data.setdefault(date, {})[metric] = value
    else:
        temp_data.setdefault(date, {})[metric] = value

# Output for the last station
if current_station_id:
    for date, metrics in temp_data.items():
        metrics = impute_temperatures(metrics)
        prcp = metrics.get('PRCP', impute_precipitation(prcp_history))
        print(
            f"{current_station_id},{get_country_by_station(current_station_id)},{date},{metrics.get('TMIN', '')},{metrics.get('TMAX', '')},{metrics.get('TAVG', '')},{prcp}")
