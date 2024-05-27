from mrjob.job import MRJob
from mrjob.step import MRStep

class MRTemperatureAnalysis(MRJob):

    def configure_args(self):
        super(MRTemperatureAnalysis, self).configure_args()
        self.add_passthru_arg('--start-date', type=str, help='Start date in YYYYMMDD format')
        self.add_passthru_arg('--end-date', type=str, help='End date in YYYYMMDD format')
        self.add_passthru_arg('--location', type=str, help='Country name or station ID')
        self.add_passthru_arg('--aggregation-unit', type=str, choices=['year', 'month', 'day'], default='day', help='Aggregation unit: year, month, or day')

    def mapper(self, _, line):
        station_id, country, date, tmin_str, tmax_str, tavg_str, _ = line.split(',')

        # Check if any temperature value is missing
        if not tmin_str or not tmax_str or not tavg_str:
            # Optionally, handle missing values or skip this record
            return

        # Check location type and filter by station ID or country
        location_filter = False
        if self.options.location in ('USA', 'CANADA', 'MEXICO'):
            location_filter = (country == self.options.location)
        elif any(self.options.location.startswith(prefix) for prefix in ['US', 'CA', 'MX']):
            location_filter = (station_id == self.options.location)

        if not location_filter:
            return
        if date < self.options.start_date or date > self.options.end_date:
            return

        # Convert temperature strings to floats
        tmin = float(tmin_str)
        tmax = float(tmax_str)
        tavg = float(tavg_str)

        # Determine the aggregation key based on the specified unit
        if self.options.aggregation_unit == 'year':
            time_key = date[:4]
        elif self.options.aggregation_unit == 'month':
            time_key = date[:6]
        elif self.options.aggregation_unit == 'day':
            time_key = date

        yield (country, time_key), (tmin, tmax, tavg)

    def reducer(self, key, values):
        tmin_sum = tmax_sum = tavg_sum = 0
        count = 0
        for tmin, tmax, tavg in values:
            tmin_sum += tmin
            tmax_sum += tmax
            tavg_sum += tavg
            count += 1
        yield key, (tmin_sum / count, tmax_sum / count, tavg_sum / count)

if __name__ == '__main__':
    MRTemperatureAnalysis.run()
