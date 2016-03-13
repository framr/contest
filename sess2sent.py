#!/usr/bin/env python
__author__ = 'fram'


import sys
import os
from argparse import ArgumentParser
from itertools import groupby


from preprocess.session import sessions_iter
from tabtools.tabutils import make_record_cls, RecordMeta

if __name__ == '__main__':

    parser = ArgumentParser()
    parser.add_argument("-i", dest="input_filename", type=str, default=None, help="input filename")
    parser.add_argument("-o", dest="output_filename", type=str, default=None, help="output filename")
    parser.add_argument("--maxint", dest="max_interval", type=int, default=300, help="maximum intreval between events in a session, seconds")
    parser.add_argument("--uniq", dest="unique", action="store_true", help="apply uniq to items within one session")
    parser.add_argument("--act", dest="act", type=str, default=None, help="grep only specific actions")

    args = parser.parse_args()
    meta_filename = args.input_filename + '.meta'
    meta = RecordMeta(open(meta_filename).readline().strip().split())
    Record = make_record_cls(meta.fields())

    with open(args.input_filename) as infile:
        with open(args.output_filename, 'w') as outfile:

            avg_session_length = 0
            session_count = 0
            for uid, lines_iter in groupby(
                infile,
                key=lambda line: line.strip().split('\t', meta['uid'] + 1)[meta['uid']]
            ):
                records = [Record(*line.strip().split('\t')) for line in lines_iter]

                for tstart, it in sessions_iter(records, args.max_interval):
                    recs = list(it)
                    session_count += 1
                    counter_id = recs[0].counter_id
                    uid_id = recs[0].uid
                    if args.act:
                        offers = [r.offer_id for r in recs if r.act == args.act]
                    else:
                        offers = [r.offer_id for r in recs]

                    if args.unique:
                        offers = set(offers)
                    avg_session_length += len(offers)
                    if offers:
                        outfile.write("%s\t%s\t%s\n" % (counter_id, uid, " ".join(offers)))

            print "processed %d sessions, avg length = %f" % (session_count, float(avg_session_length) / session_count)
