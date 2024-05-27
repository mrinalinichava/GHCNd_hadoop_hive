from mrjob.job import MRJob
import datetime

class PrecipitationAnalysisMRJob(MRJob):

    # define command line args
    def configure_args(self):
        super(PrecipitationAnalysisMRJob, self).configure_args()
        self.add_passthru_arg('--start-date', type=str, help='Start date in YYYYMMDD format', required=True)
        self.add_passthru_arg('--end-date', type=str, help='End date in YYYYMMDD format', required=True)
        self.add_passthru_arg('--unit-of-time', type=str, default='daily', choices=['daily', 'monthly', 'yearly'], help='Unit of time for aggregation')
        self.add_passthru_arg('--location', type=str, help='Location to filter data by (can be station ID or country name)', required=True)

    # Mapper Function
    def mapper(self, _, line):
        # Split the input line into parts
        parts = line.split(',')
        if len(parts) < 7: return  # Skip malformed lines

        # Extract relevant info from the line
        station_id, country, date_str, _, _, _, precipitation_str = parts

        # Filter by location: match either the station_id or the country to the --location argument
        if self.options.location != station_id and self.options.location != country:
            return

        try:
            precipitation = float(precipitation_str)
        except ValueError:
            return  # Skip lines with invalid precipitation value

        date = datetime.datetime.strptime(date_str, '%Y%m%d').date()
        start_date = datetime.datetime.strptime(self.options.start_date, '%Y%m%d').date()
        end_date = datetime.datetime.strptime(self.options.end_date, '%Y%m%d').date()

        if not (start_date <= date <= end_date):
            return  # Skip records outside the date range

        # Aggregate data based on the unit of time
        if self.options.unit_of_time == 'daily':
            yield (country, date_str), precipitation
        elif self.options.unit_of_time == 'monthly':
            yield (country, date_str[:6]), precipitation  # Year and month
        elif self.options.unit_of_time == 'yearly':
            yield (country, date_str[:4]), precipitation  # Year

    # reducer func
    def reducer(self, key, values):
        country, date = key
        values_list = list(values)
        if values_list:  # Ensure there are values to avoid division by zero
            average_precipitation = sum(values_list) / len(values_list)
            yield (country, date), round(average_precipitation, 2)  # Round the average to two decimal places
        else:
            yield (country, date), 0.00  # Yield 0.00 if no values

if __name__ == '__main__':
    PrecipitationAnalysisMRJob.run()



