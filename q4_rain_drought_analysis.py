#
# from mrjob.job import MRJob
# import datetime
#
# class RainfallDroughtAnalysisMRJob(MRJob):
#
#     def configure_args(self):
#         super(RainfallDroughtAnalysisMRJob, self).configure_args()
#         self.add_passthru_arg('--start-date', type=str, help='Start date in YYYYMMDD format', required=False)
#         self.add_passthru_arg('--end-date', type=str, help='End date in YYYYMMDD format', required=False)
#         self.add_passthru_arg('--rainfall-threshold', type=float, help='Threshold for heavy rainfall (mm/day)', required=True)
#         self.add_passthru_arg('--drought-threshold', type=float, help='Threshold for drought conditions (mm/day)', required=True)
#
#     def mapper(self, _, line):
#         parts = line.split(',')
#         if len(parts) < 7: return  # Skip malformed lines
#
#         station_id, country, date_str, _, _, _, precipitation_str = parts
#
#         try:
#             precipitation = float(precipitation_str)
#             date = datetime.datetime.strptime(date_str, '%Y%m%d').date()
#         except ValueError:
#             return  # Skip lines with invalid data
#
#         # Optionally filter by date range
#         if self.options.start_date:
#             start_date = datetime.datetime.strptime(self.options.start_date, '%Y%m%d').date()
#             if date < start_date:
#                 return
#
#         if self.options.end_date:
#             end_date = datetime.datetime.strptime(self.options.end_date, '%Y%m%d').date()
#             if date > end_date:
#                 return
#
#         # Determine the condition based on precipitation thresholds
#         if precipitation >= self.options.rainfall_threshold:
#             condition = 'Heavy Rainfall'
#         elif precipitation <= self.options.drought_threshold:
#             condition = 'Drought'
#         else:
#             condition = 'Normal'
#
#         yield (country, condition), precipitation
#
#     def reducer(self, key, values):
#         precipitation_values = list(values)
#         if precipitation_values:
#             avg_precipitation = sum(precipitation_values) / len(precipitation_values)
#         else:
#             avg_precipitation = 0
#         yield key, round(avg_precipitation, 2)
#
# if __name__ == '__main__':
#     RainfallDroughtAnalysisMRJob.run()








from mrjob.job import MRJob
import datetime

class RainfallDroughtAnalysisMRJob(MRJob):

    # command line args
    def configure_args(self):
        super(RainfallDroughtAnalysisMRJob, self).configure_args()
        self.add_passthru_arg('--start-date', type=str, help='Start date in YYYYMMDD format', required=False)
        self.add_passthru_arg('--end-date', type=str, help='End date in YYYYMMDD format', required=False)
        self.add_passthru_arg('--rainfall-threshold', type=float, help='Threshold for heavy rainfall (mm/day)', required=True)
        self.add_passthru_arg('--drought-threshold', type=float, help='Threshold for drought conditions (mm/day)', required=True)

    # Mapper function
    def mapper(self, _, line):
        parts = line.split(',')
        # Skip malformed lines
        if len(parts) < 7: return

        station_id, country, date_str, _, _, _, precipitation_str = parts

        try:
            precipitation = float(precipitation_str)
            date = datetime.datetime.strptime(date_str, '%Y%m%d').date()
        except ValueError:
            return  # Skip lines with invalid data

        # Optionally filter by date range
        if self.options.start_date:
            start_date = datetime.datetime.strptime(self.options.start_date, '%Y%m%d').date()
            if date < start_date:
                return

        if self.options.end_date:
            end_date = datetime.datetime.strptime(self.options.end_date, '%Y%m%d').date()
            if date > end_date:
                return

        # Classify the precipitation level
        if precipitation >= self.options.rainfall_threshold:
            condition = 'Heavy Rainfall'
        elif precipitation <= self.options.drought_threshold:
            condition = 'Drought'
        else:
            condition = 'Normal'

        yield (country, date_str, condition), 1

# Reducer function
    def reducer(self, key, values):
        # extract key components
        country, date_str, condition = key
        # sum up the values to get the count
        count = sum(values)
        yield (country, condition), (date_str, count)

if __name__ == '__main__':
    RainfallDroughtAnalysisMRJob.run()


# from mrjob.job import MRJob
# import datetime
#
# class RainfallDroughtAnalysisMRJob(MRJob):
#
#     def configure_args(self):
#         super(RainfallDroughtAnalysisMRJob, self).configure_args()
#         self.add_passthru_arg('--start-date', type=str, help='Start date in YYYYMMDD format', required=False)
#         self.add_passthru_arg('--end-date', type=str, help='End date in YYYYMMDD format', required=False)
#         self.add_passthru_arg('--rainfall-threshold', type=float, help='Threshold for heavy rainfall (mm/day)', required=True)
#         self.add_passthru_arg('--drought-threshold', type=float, help='Threshold for drought conditions (mm/day)', required=True)
#
#     def mapper(self, _, line):
#         parts = line.split(',')
#         if len(parts) < 7: return  # Skip malformed lines
#
#         station_id, country, date_str, _, _, _, precipitation_str = parts
#
#         try:
#             precipitation = float(precipitation_str)
#             date = datetime.datetime.strptime(date_str, '%Y%m%d').date()
#         except ValueError:
#             return  # Skip lines with invalid data
#
#         # Optionally filter by date range
#         if self.options.start_date:
#             start_date = datetime.datetime.strptime(self.options.start_date, '%Y%m%d').date()
#             if date < start_date:
#                 return
#
#         if self.options.end_date:
#             end_date = datetime.datetime.strptime(self.options.end_date, '%Y%m%d').date()
#             if date > end_date:
#                 return
#
#         # Determine the condition based on precipitation thresholds
#         if precipitation >= self.options.rainfall_threshold:
#             condition = 'Heavy Rainfall'
#         elif precipitation <= self.options.drought_threshold:
#             condition = 'Drought'
#         else:
#             condition = 'Normal'
#
#         yield (country, condition), (str(date), precipitation)
#
#     def reducer(self, key, values):
#         dates_and_precipitation = list(values)
#         yield key, dates_and_precipitation
#
# if __name__ == '__main__':
#     RainfallDroughtAnalysisMRJob.run()
