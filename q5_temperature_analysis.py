from mrjob.job import MRJob
import datetime


class TemperatureTrendsAnalysis(MRJob):

    def mapper(self, _, line):
        # Assuming the data format: station_id, country_state, date, tmin, tmax, tavg, precipitation
        parts = line.split(',')
        if len(parts) < 7: return  # Skip malformed lines

        _, country_state, date_str, tmin_str, tmax_str, tavg_str, _ = parts
        year_month = date_str[:6]  # YYYYMM format

        try:
            tmin = float(tmin_str)
            tmax = float(tmax_str)
            tavg = float(tavg_str)
        except ValueError:
            return  # Skip lines with invalid temperature values

        yield (country_state, year_month), (tmin, tmax, tavg)

    def reducer(self, key, values):
        temperatures = list(values)
        tmin = min(t[0] for t in temperatures)
        tmax = max(t[1] for t in temperatures)
        tavg = sum(t[2] for t in temperatures) / len(temperatures)

        yield key, {'min_temp': tmin, 'max_temp': tmax, 'avg_temp': round(tavg, 2)}


if __name__ == '__main__':
    TemperatureTrendsAnalysis.run()
