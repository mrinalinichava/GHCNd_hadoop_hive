from mrjob.job import MRJob

class PrecipitationTrendsAnalysis(MRJob):

    def mapper(self, _, line):
        # Assuming the data format: station_id, country_state, date, tmin, tmax, tavg, precipitation
        parts = line.split(',')
        if len(parts) < 7: return  # Skip malformed lines

        _, country_state, date_str, _, _, _, precipitation_str = parts
        year_month = date_str[:6]  # YYYYMM format

        try:
            precipitation = float(precipitation_str)
        except ValueError:
            return  # Skip lines with invalid precipitation values

        yield (country_state, year_month), precipitation

    def reducer(self, key, values):
        precipitation_values = list(values)
        if precipitation_values:
            avg_precipitation = sum(precipitation_values) / len(precipitation_values)
        else:
            avg_precipitation = 0
        yield key, round(avg_precipitation, 2)

if __name__ == '__main__':
    PrecipitationTrendsAnalysis.run()
