#!/usr/bin/env python
__author__ = 'fram'
import os
import sys
from itertools import groupby
from datetime import datetime

from tabtools.tabutils import make_record_cls, RecordMeta
from preprocess.session import sessions_iter
from tabtools.tabutils import dump_records

def find_last_sessions(records, max_interval):
    '''
    :param records:
    :return: list of records in last session
    '''

    last_session = []
    last_session_tstart = None
    for start_time, session in sessions_iter(records, max_interval):
        last_session = list(session)
        last_session_tstart = start_time
    last_session_records = [rec for rec in last_session]
    return last_session_tstart, last_session_records

def find_interacted_items(records):
    items = set([rec.offer_id for rec in records])
    return items

def filter_items(records, items):
    return [rec for rec in records if rec.offer_id not in items]




def split_learn_test(filename, meta, test_period, max_session_interval, learn_name='learn', test_name='test'):
    """

    :param filename: input file
    :param meta: meta information - description of fields in input file
    :param test_period: period within which the test session may lay
    :param learn_name:
    :param test_name:
    :return:
    """

    Record = make_record_cls(meta.fields())
    with open(filename) as infile:
        with open(learn_name, 'w') as learn_file:
            with open(test_name, 'w') as test_file:
                for uid, lines_iter in groupby(
                        infile,
                        key=lambda line: line.strip().split('\t', meta['uid'] + 1)[meta['uid']]
                ):
                    records = [Record(*line.strip().split('\t')) for line in lines_iter]
                    test_records = [rec for rec in records if rec.timestamp > test_period[0] and rec.timestamp < test_period[1]]
                    test_tstart, test_records = find_last_sessions(test_records, max_session_interval)
                    #print "uid = %s  tstart = %s" % (uid, test_tstart)
                    #print len(test_records)

                    if test_records:
                        test_tstart = test_tstart.strftime("%Y-%m-%d %H:%M:%S")
                        # leave learn records only
                        records = [rec for rec in records if rec.timestamp < test_tstart]
                        interacted_items = find_interacted_items(records)
                        test_records = filter_items(test_records, interacted_items)
                        dump_records(test_file, test_records)
                    dump_records(learn_file, records)




if __name__ == '__main__':
    pass
