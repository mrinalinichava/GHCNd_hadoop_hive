from mrjob.job import MRJob
from mrjob.step import MRStep


class TemperatureComparison(MRJob):

    def configure_args(self):
        super(TemperatureComparison, self).configure_args()
        self.add_passthru_arg('--region1', help='First region for comparison')
        self.add_passthru_arg('--region2', help='Second region for comparison')

    def mapper(self, _, line):
        parts = line.strip().split(',')
        if len(parts) < 7:
            return  # Skip malformed lines

        station_id, country, date, tmin, tmax, tavg, prcp = parts

        # Check if tavg is a valid number; if not, skip or handle accordingly
        try:
            tavg_float = float(tavg)
        except ValueError:
            # tavg is not a valid float, so skip this record or handle as needed
            return

        if country in (self.options.region1, self.options.region2):
            year = date[:4]  # Extract the year for aggregation
            yield (country, year), tavg_float

    def reducer(self, key, values):
        total_temp = count = 0
        for temp in values:
            total_temp += temp
            count += 1
        avg_temp = total_temp / count if count else None
        yield key, avg_temp


if __name__ == '__main__':
    TemperatureComparison.run()
