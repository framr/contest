#!/usr/bin/env python
__author__ = 'fram'

import sys
import os
import mmh3
from collections import defaultdict


from tabtools.tabutils import RecordMeta, make_record_cls

if __name__ == '__main__':

    input_filename = sys.argv[1]
    meta_filename = input_filename + '.meta'
    #output_filename = sys.argv[2]

    meta = RecordMeta(open(meta_filename).readline().strip().split())
    Record = make_record_cls(meta.fields())
    stats = {}
    offer_map = {}
    for line in open(input_filename):
        splitted = line.strip().split('\t')
        rec = Record(*splitted)
        offers = [offer for offer in rec.offers.strip().split() if offer.isdigit()]
        for offer in offers:
            stats[offer] = stats.get(offer, 0) + 1

    for offer in sorted(stats, key=stats.__getitem__, reverse=True):
        print "%s\t%s\t%s\t%s" % (offer, stats[offer])

