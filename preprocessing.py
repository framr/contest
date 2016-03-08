#!/usr/bin/env python

import sys
import os
import mmh3


from tabutils import RecordMeta, make_record_cls



def read_feature_map(filename):
    pass

def read_sampling_table(filename)
    pass


def preprocess_items(filename, outfilename):
    """
    :param filename:
    :param outfilename:
    :return:
    """

    meta = RecordMeta(open(filename).readline().strip().split())
    Record = make_record_cls(meta)

    with open(outfilename, 'w'):
        for line in open(filename):
            rec = Record(line.strip().split('\t'))
            items = [offer for offer in rec.offers.split() if offer.isdigit()]
            if not items:
                continue
            counter_id = rec.counter_id
            new_items = [mmh3("%s_%s" % (counter_id, item)) for item in items]
            new_rec_data = dict([(f, getattr(rec, f)) for f in meta.fields()])
            new_rec_data['offers'] = new_items

            new_rec = '\t'.join([new_rec_data[field] for field in meta.fields()])
            outfilename.write("%s\n" % new_rec)

























































        rec = Record(line.strip().split('\t'))




if __name__ == '__main__':
    pass

