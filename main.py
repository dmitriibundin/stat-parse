#!/usr/bin/env python3

from collections import OrderedDict
from functools import partial
import argparse
import sys
import subprocess
import io
import os
import time

import parser
import data_io

DEFAULT_COLUMN_VALUE=''

def parse_extra_columns(kvs):
    extra_columns=[]
    for kv_str in kvs.split(','):
        kv=kv_str.split(':', 1) 
        if len(kv) != 2 or not all(kv):
            raise Exception(f'extra-columns should be of the form KEY:VALUE. Got "{kv_str}"')
        extra_columns.append(tuple(kv))
    return extra_columns

def parse_int_ge(threshold, cnt):
    int_value=int(cnt)
    if int_value < threshold:
        #Todo: which value?
        raise Exception(f'Value greater or equal then {threshold}. Got {int_value}')
    return int_value

def uncaught_exception_logger(ex_type, value, traceback):
    print(f'Error: {str(value)}')
    sys.exit(1)

if __name__ == '__main__':
    sys.excepthook=uncaught_exception_logger
    run_options=argparse.ArgumentParser('Run configuration')
    run_options.add_argument('--pause', type=partial(parse_int_ge, 0), help='Milliseconds to wait between different runs', default=0)
    run_options.add_argument('--run-count', type=partial(parse_int_ge, 1), help='Number of iteration to run a supplied command', default=1)
    run_options.add_argument('--output', default='output.csv', type=str, help='File path to write data to')
    run_options.add_argument('--extra-columns', type=parse_extra_columns, default=[], help='Extra columns with a specific value to append to the result: K:V[,K:V]')
    run_options.add_argument(nargs=argparse.REMAINDER, dest='command')
    args=run_options.parse_args(sys.argv[1:])

    iteration_count=args.run_count
    extra_columns=args.extra_columns
    output_path=args.output
    pause_millis=args.pause

    result = [[] for _ in range(iteration_count)]
    for i in range(iteration_count):
        p = subprocess.Popen(args.command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        for line in io.TextIOWrapper(p.stderr):
            result[i].append(line)
        if not result[i]:
            #TODO: It is undesirable to add empty list. Consider adding iteration column
            print(f'Encountered empty stderr for iteration {i}. Is data writing to stdout?', file=sys.stderr)
        else:
            print(f'Counters for iteration {i} collected\r', file=sys.stderr, end='', flush=True)
        if pause_millis > 0:
            time.sleep(pause_millis / 1000)
    print()

    def add_extra_column(list_of_lists, extra_column_tuple):
        return [lst.append(extra_column_tuple) for lst in list_of_lists]

    perf_stats=[parser.parse_perf_stat_csv(raw_stat) + extra_columns for raw_stat in result]
    perf_stats_schema=parser.get_stat_schema(perf_stats)

    def values_with_defaults(keys, default_value, stats):
        keys_with_defaults=[(key, default_value) for key in keys]
        stats_with_keys=list((OrderedDict(keys_with_defaults + stats).items()))
        retval=[stat[1] for stat in stats_with_keys]
        return retval

    output_path_existing_schema=data_io.read_schema(output_path, field_separator=',')
    #TODO: This is incorrect in case output_path_existing_schema has an order that differs from perf_stats_schema
    if output_path_existing_schema and set(output_path_existing_schema) != set(perf_stats_schema):
        while os.path.exists(output_path):
            output_path += '_1'
        output_path_existing_schema=[]
        print(f'Output path schema differs. Output = "{output_path}"', file=sys.stderr)
            
    perf_stats_data=[values_with_defaults(perf_stats_schema, DEFAULT_COLUMN_VALUE, stat) for stat in perf_stats]

    data_io.append_to_csv(output_path, [] if output_path_existing_schema else [perf_stats_schema], field_separator=',')
    data_io.append_to_csv(output_path, perf_stats_data, field_separator=',')
