from datetime import datetime

current_datetime = datetime.now()
print(current_datetime)
print(current_datetime.year)            # 2019
print(current_datetime.month)           # 12
print(current_datetime.day)             # 13
print(current_datetime.hour)            # 12
print(current_datetime.minute)          # 18
print(current_datetime.second)          # 18
print(current_datetime.microsecond)

def create_partition_path(input_datetime :datetime):
    partition_str = "year={},{},{},{}".format(input_datetime.year,input_datetime.month, input_datetime.day, input_datetime.hour)
    print(partition_str)

create_partition_path(current_datetime)