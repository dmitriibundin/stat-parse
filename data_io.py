from collections import OrderedDict
import csv

#data_list is a list of lists of tuples.
#
#Each of the tuples has the form ('key': 'value').
#
#There might be keys presented only in a few elements in a list.
#In this case the default_value is written
def append_to_csv(output_path, data_list, field_separator):
    with open(output_path, 'a') as fd:
        stat_csv_writer=csv.writer(fd, delimiter=field_separator)
        for data in data_list:
            stat_csv_writer.writerow(data)

def read_schema(ouput_path, field_separator):
    try:
        with open(ouput_path) as fd:
            return next(iter(csv.reader(fd, delimiter=field_separator)), [])
    #TODO: looks weird
    except FileNotFoundError:
        return []
