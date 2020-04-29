from functools import reduce
from collections import OrderedDict
import csv

FIELD_SEPARATOR=','
#Parsing CSV perf output with --field-separator=,
def parse_perf_stat_csv(data):
    perf_stat=csv.reader(data, delimiter=FIELD_SEPARATOR)
    result=[]
    for row in perf_stat:
        result.append((row[2], row[0]))
    
    return result

def get_stat_schema(stats):
    all_stat_keys=reduce(lambda result, current: result + [val[0] for val in current], stats, [])
    csv_schema = list(OrderedDict.fromkeys(all_stat_keys))
    return csv_schema
