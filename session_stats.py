#!/usr/bin/env python
__author__ = 'fram'

import sys
import os
import mmh3
from collections import defaultdict


from tabutils import RecordMeta, make_record_cls

if __name__ == '__main__':

    input_filename = sys.argv[1]
    meta_filename = input_filename + '.meta'
    #output_filename = sys.argv[2]

    meta = RecordMeta(open(meta_filename).readline().strip().split())
    Record = make_record_cls(meta.fields())
    stats = {}
    errors = 0
    for line in open(input_filename):
        splitted = line.strip().split('\t')
        if len(splitted) != 3:
            errors += 1
            continue
        rec = Record(*splitted)
        offers = [offer for offer in rec.offers.strip().split()]
        stats[len(offers)] = stats.get(len(offers), 0) + 1
        #for offer in offers:
        #    stats[rec.counter_id][offer] = stats[rec.counter_id].get(offer, 0) + 1

    for num in sorted(stats, key=stats.__getitem__, reverse=True):
        print "%s\t%s" % (num, stats[num])

    print "found %d errors" % errors
