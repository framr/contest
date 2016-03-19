#!/usr/bin/env python

import sys
import os
import mmh3


from tabtools.tabutils import RecordMeta, make_record_cls



def read_feature_map(filename):
    pass

def read_sampling_table(filename):
    pass






def remap_items(filename, outfilename, feature_map, offer_field='offers', enumerate=False):
    """
    Remap items
    :param filename:
    :param outfilename:
    :return:
    """
    meta = RecordMeta(open(filename + '.meta').readline().strip().split())
    Record = make_record_cls(meta.fields())

    mapping = {}
    with open(outfilename, 'w') as outfile:
        for line in open(filename):
            splitted = line.strip().split('\t')
            rec = Record(*splitted)
            items = [offer for offer in getattr(rec, offer_field).split() if offer.isdigit()]
            if not items:
                continue
            counter_id = rec.counter_id

            new_items = []
            for item in items:
                offer_hash = mmh3.hash128("%s_%s" % (counter_id, item))

                if offer_hash not in mapping:
                    index = len(mapping)
                    mapping[offer_hash] = (index, counter_id, item)
                else:
                    index = mapping[offer_hash][0]

                if enumerate:
                    new_items.append(str(index))
                else:
                    new_items.append(str(offer_hash))



            new_rec_data = dict([(f, getattr(rec, f)) for f in meta.fields()])
            new_rec_data[offer_field] = ' '.join(new_items)


            new_rec = '\t'.join([new_rec_data[field] for field in meta.fields()])
            outfile.write("%s\n" % new_rec)


    with open(feature_map, 'w') as fmap:
        for offer_hash, (i, counter_id, item) in mapping.iteritems():
            fmap.write("%s\t%s\t%s\t%s\n" % (offer_hash, i, counter_id, item))

    with open(feature_map + '.meta', 'w') as fmap:
        fmap.write("offer_hash\tmap\tcounter_id\toffer_id\n")


if __name__ == '__main__':
    pass

