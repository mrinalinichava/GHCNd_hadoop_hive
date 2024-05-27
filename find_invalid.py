
from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime, timedelta

class IdentifyInvalidStations(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        fields = line.split(',')
        station_id, date, temp_min, temp_max, temp_avg, prec = fields[0], fields[2], fields[3], fields[4], fields[5], fields[6]
        temp_data_missing = temp_min == '' or temp_max == '' or temp_avg == ''
        prec_data_missing = prec == ''
        yield station_id, (date, temp_data_missing, prec_data_missing)

    def reducer(self, station_id, values):
        sorted_values = sorted(values, key=lambda x: datetime.strptime(x[0], "%Y%m%d"))
        missing_count = 0
        last_date = None

        for date, temp_missing, prec_missing in sorted_values:
            date_obj = datetime.strptime(date, "%Y%m%d")
            if last_date and date_obj == last_date + timedelta(days=1) and (temp_missing or prec_missing):
                missing_count += 1
            else:
                missing_count = 0 if not temp_missing and not prec_missing else 1

            if missing_count > 10:
                yield station_id, "invalid"
                return  # Stop processing once a station is marked invalid
            last_date = date_obj

if __name__ == '__main__':
    IdentifyInvalidStations.run()






# from mrjob.job import MRJob
# from mrjob.step import MRStep
# from datetime import datetime, timedelta
#
#
# class IdentifyValidStations(MRJob):
#
#     def steps(self):
#         return [
#             MRStep(mapper=self.mapper,
#                    reducer=self.reducer)
#         ]
#
#     def mapper(self, _, line):
#         fields = line.split(',')
#         station_id, date, temp_min, temp_max, temp_avg, prec = fields[0], fields[2], fields[3], fields[4], fields[5], \
#         fields[6]
#         temp_data_missing = temp_min == '' or temp_max == '' or temp_avg == ''
#         prec_data_missing = prec == ''
#         yield station_id, (date, temp_data_missing, prec_data_missing)
#
#     def reducer(self, station_id, values):
#         sorted_values = sorted(values, key=lambda x: datetime.strptime(x[0], "%Y%m%d"))
#         missing_count = 0
#         last_date = None
#
#         for date, temp_missing, prec_missing in sorted_values:
#             date_obj = datetime.strptime(date, "%Y%m%d")
#             if last_date and date_obj == last_date + timedelta(days=1) and (temp_missing or prec_missing):
#                 missing_count += 1
#             else:
#                 missing_count = 0 if not temp_missing and not prec_missing else 1
#
#             if missing_count > 10:
#                 yield station_id, "invalid"
#                 return
#             last_date = date_obj
#
#         yield station_id, "valid"
#
#
# if __name__ == '__main__':
#     IdentifyValidStations.run()


from mrjob.job import MRJob
from datetime import datetime, timedelta


# class IdentifyInconsistentStations(MRJob):
#     def mapper(self, _, line):
#         # Split the line into fields
#         fields = line.split(',')
#         station_id, date, temp_min, temp_max, temp_avg, prec = fields[0], fields[2], fields[3], fields[4], fields[5], \
#         fields[6]
#
#         # Check for missing data
#         missing_data = temp_min == '' or temp_max == '' or temp_avg == '' or prec == ''
#         yield station_id, (date, missing_data)
#
#     def reducer(self, station_id, values):
#         sorted_values = sorted(values, key=lambda x: datetime.strptime(x[0], "%Y%m%d"))
#
#         # Counter for consecutive days with missing data
#         consecutive_missing = 0
#         last_date = None
#
#         for date, missing in sorted_values:
#             date_obj = datetime.strptime(date, "%Y%m%d")
#             if last_date is not None and date_obj == last_date + timedelta(days=1):
#                 if missing:
#                     consecutive_missing += 1
#                     if consecutive_missing > 10:
#                         yield station_id, 1
#                         break
#                 else:
#                     consecutive_missing = 0
#             else:
#                 consecutive_missing = 0 if not missing else 1
#             last_date = date_obj
#
#
# if __name__ == '__main__':
#     IdentifyInconsistentStations.run()
