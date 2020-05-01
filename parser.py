from functools import reduce
from collections import OrderedDict
import csv

FIELD_SEPARATOR=','
#Parsing CSV perf output with --field-separator=,
def parse_perf_stat_csv(data):
    perf_stat=csv.reader(data, delimiter=FIELD_SEPARATOR)
    result=[]
    #TODO: Handle not enough value to unpack
    for perf_counter_value, _, perf_event_name, *etc in perf_stat:
        result.append((perf_event_name, perf_counter_value))
    
    return result

def get_stat_schema(stats):
    return list(
            OrderedDict.fromkeys(
                [perf_event_name for stat in stats for perf_event_name, perf_counter_value in stat]
                )
            )
