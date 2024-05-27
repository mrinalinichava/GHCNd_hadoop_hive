#!/usr/bin/env python3

import sys

# Initialize an empty set to hold the excluded station IDs
excluded_stations = set()

# Pass the excluded stations file as the first argument to the script
excluded_stations_file = sys.argv[1]

# Load the list of excluded stations into a set for quick lookup
with open(excluded_stations_file, "r") as f:
    for line in f:
        # Each line is in the format: "station_id" "invalid"
        # Extract the station ID by splitting the line and removing the quotes
        station_id = line.split()[0].strip('"')
        excluded_stations.add(station_id)

# Process each line from the input dataset
for line in sys.stdin:
    line = line.strip()
    parts = line.split(',')

    # Check if the station ID is not in the excluded list, then print the line
    if parts[0] not in excluded_stations:
        print(line)
