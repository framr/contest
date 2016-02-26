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
    stats = defaultdict(dict)
    for line in open(input_filename):
        rec = Record(*line.strip().split())
        offers = [offer for offer in rec.offers.strip().split()]
        for offer in offers:
            stats[rec.counter_id][offer] = stats[rec.counter_id].get(offer, 0) + 1

    for counter, counter_data in stats.iteritems():
        for offer in sorted(counter_data, key=counter_data.__getitem__, reverse=True):
            print "%s\t%s\t%s\n" % (counter, offer, mmh3.hash128("%s_%s" % ()), counter_data[offer])
