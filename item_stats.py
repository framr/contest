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
    offer_map = {}
    for line in open(input_filename):
        splitted = line.strip().split('\t')
        rec = Record(*splitted)
        offers = [offer for offer in rec.offers.strip().split() if offer.isdigit()]
        for offer in offers:
            full_offer = "%s_%s" % (rec.counter_id, offer)
            offer_map[full_offer] = (rec.counter_id, offer)
            stats[full_offer] = stats.get(full_offer, 0) + 1

    for i, offer in enumerate(sorted(stats, key=stats.__getitem__, reverse=True)):
        offer_hash = mmh3.hash128("%s_%s" % (offer_map[offer][0], offer_map[offer][1]))
        print "%d\t%s\t%s\t%s\t%s" % (i, offer_map[offer][0], offer_map[offer][1], offer_hash, stats[offer])


    #mmh3.hash128("%s_%s" % ()), counter_data[offer])
