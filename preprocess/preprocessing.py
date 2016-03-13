#!/usr/bin/env python

import sys
import os
import mmh3


from tabtools.tabutils import RecordMeta, make_record_cls



def read_feature_map(filename):
    pass

def read_sampling_table(filename):
    pass


def remap_items(filename, outfilename, feature_map):
    """
    Remap items
    :param filename:
    :param outfilename:
    :return:
    """

    meta = RecordMeta(open(filename).readline().strip().split())
    Record = make_record_cls(meta)

    mapping = {}
    with open(outfilename, 'w'):
        for line in open(filename):
            rec = Record(line.strip().split('\t'))
            items = [offer for offer in rec.offers.split() if offer.isdigit()]
            if not items:
                continue
            counter_id = rec.counter_id

            new_items = []
            for item in items:
                offer_hash = mmh3("%s_%s" % (counter_id, item))
                new_items.append(offer_hash)
                mapping[offer_hash] = (counter_id, item)


            new_rec_data = dict([(f, getattr(rec, f)) for f in meta.fields()])
            new_rec_data['offers'] = new_items


            new_rec = '\t'.join([new_rec_data[field] for field in meta.fields()])
            outfilename.write("%s\n" % new_rec)


    with open(feature_map, 'w') as fmap:
        for i, (offer_hash, (counter_id, item)) in enumerate(mapping.iteritems()):
            fmap.write("%d\t%d\t%d\t%d\n" % (offer_hash, i, counter_id, item))

    with open(feature_map + '.meta', 'w') as fmap:
        fmap.write("offer_hash\tmap\tcounter_id\toffer_id\n")































































        rec = Record(line.strip().split('\t'))




if __name__ == '__main__':
    pass

