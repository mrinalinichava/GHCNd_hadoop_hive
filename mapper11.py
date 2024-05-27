#!/usr/bin/env python3
import sys

def read_input(file):
    for line in file:
        yield line.strip()

def is_valid_station(station_id):
    return station_id.startswith(('US', 'CA', 'MX'))

def main():
    for line in read_input(sys.stdin):
        parts = line.split(',')
        if len(parts) < 7:  # Ensure we have the expected number of columns
            continue

        station_id, date, metric, value, m_flag, q_flag, s_flag, obs_time = parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], parts[6], parts[7] if len(parts) == 8 else ''

        # Filter out stations not in North America,Mexico or Canada
        if is_valid_station(station_id):
            print(f"{station_id},{date}\t{metric},{value}")

if __name__ == "__main__":
    main()
